from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import os
import threading
import time
from http import HTTPStatus
from typing import List, Dict, Any, Optional
from zoneinfo import ZoneInfo

import requests

BASE = "https://www.googleapis.com/youtube/v3"

@dataclass
class _QuotaConfig:
    daily_cap: int = 10_000
    tz: str = "Australia/Melbourne"
    unit_costs: dict = field(default_factory=lambda: {
        # High Cost
        "search.list": 100,
        # Common reads
        "videos.list": 1,
        "channels.list": 1,
        "playlistItems.list": 1,
        "subscriptions.list": 1,
        "commentThreads.list": 1,
        "comments.list": 1,
        # Less common and pricier
        "captions.list": 50,
        "thubmnails.set": 50
    })

class QuotaExceeded(Exception):
    pass

class YoutubeQuotaManager:
    def __init__(self):
        self.quota_remaining = 10_000
        self.quota_reset_time = time.time()
        self.quota_lock = threading.Lock()

        self._config = _QuotaConfig()
        self._tz = ZoneInfo(self._config.tz)
        self._daily_cap = self._config.daily_cap
        self._unit_costs = dict(self._config.unit_costs)

        # initialise to next lcoal midnight
        self._set_next_reset_to_local_midnight()
    
    # ---------- Public API ---------- #
    def set_daily_cap(self, cap: int) -> None:
        with self.quota_lock:
            self._maybe_reset_locked()
            self._daily_cap = int(cap)
            self.quota_remaining = min(self.quota_remaining, self._daily_cap)
        
    def set_cost(self, endpoint: str, units: int) -> None:
        with self.quota_lock:
            self._unit_costs[endpoint] = int(units)

    def estimate_cost(self, endpoint: str, *, pages: int = 1, cost_override: int | None = None) -> int:
        cost = cost_override if cost_override is not None else self._unit_costs.get(endpoint, 1)
        return max(cost, 0) * max(int(pages), 1)

    def will_fit(self, endpoint: str, *, pages: int = 1, cost_override: int | None = None) -> bool:
        need = self.estimate_cost(endpoint, pages=pages, cost_override=cost_override)
        with self.quota_lock:
            self._maybe_reset_locked()
            return self.quota_remaining >= need
    
    def record(self, endpoint: str, *, pages: int = 1, cost_override: int | None = None, strict: bool = True) -> int:
        add = self.estimate_cost(endpoint, pages=pages, cost_override=cost_override)
        with self.quota_lock:
            self._maybe_reset_locked()
            if strict and self.quota_remaining < add:
                raise QuotaExceeded(
                    f"Need {add} units for {endpoint} (pages={pages}) "
                    f"but only {self.quota_remaining}/{self._daily_cap} remain."
                )
            self.quota_remaining = max(self.quota_remaining - add, 0)
            return add  # units charged
    
    @contextmanager
    def use(self, endpoint: str, *, pages: int = 1, cost_override: int | None = None, strict: bool = True):
        """Reserve units before your API call. Rolls back if your code throws."""
        charged = self.record(endpoint, pages=pages, cost_override=cost_override, strict=strict)
        try:
            yield
        except Exception:
            # Roll back the reservation if the call failed
            with self.quota_lock:
                self._maybe_reset_locked()
                self.quota_remaining = min(self.quota_remaining + charged, self._daily_cap)
            raise
    
    def remaining(self) -> int:
        with self.quota_lock:
            self._maybe_reset_locked()
            return self.quota_remaining

    def used(self) -> int:
        with self.quota_lock:
            self._maybe_reset_locked()
            return self._daily_cap - self.quota_remaining

    def reset_now(self) -> None:
        """Force a reset (rarely needed)."""
        with self.quota_lock:
            self.quota_remaining = self._daily_cap
            self._set_next_reset_to_local_midnight()

    # -------- Internals --------
    def _set_next_reset_to_local_midnight(self) -> None:
        now = datetime.now(self._tz)
        tomorrow = (now + timedelta(days=1)).date()
        next_midnight = datetime.combine(tomorrow, datetime.min.time(), self._tz)
        self.quota_reset_time = next_midnight.timestamp()

    def _maybe_reset_locked(self) -> None:
        now_ts = time.time()
        if now_ts >= self.quota_reset_time:
            self.quota_remaining = self._daily_cap
            self._set_next_reset_to_local_midnight()

# Global quota manager instance
_quota_manager = YoutubeQuotaManager()

def _get_api_key() -> str:
    """Get YouTube API key from environment, with helpful error message."""
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise ValueError(
            "YOUTUBE_API_KEY is not set. Please set this environment variable.\n"
            "Get your API key from: https://console.cloud.google.com/apis/credentials"
        )
    return api_key


# ---------- low-level helper ----------

def _get_with_retry(path: str, params: Dict[str, Any], max_retries: int = 3, base: str = BASE) -> Dict[str, Any]:
    """
    GET with simple retry/backoff on 429 and 5xx.
    Raises on persistent failure.
    """
    # Check quota before making the call
    endpoint = f"{path}.list" if not path.endswith('.list') else path
    
    if not _quota_manager.will_fit(endpoint):
        raise QuotaExceeded(f"Insufficient quota for {endpoint}. Remaining: {_quota_manager.remaining()}")
    
    api_key = _get_api_key()  # Get API key dynamically
    params = {**params, "key": api_key}
    backoff = 2
    
    with _quota_manager.use(endpoint):
        for attempt in range(1, max_retries + 1):
            resp = requests.get(f"{base}/{path}", params=params, timeout=15)
            if resp.status_code in (HTTPStatus.TOO_MANY_REQUESTS, HTTPStatus.INTERNAL_SERVER_ERROR,
                                    HTTPStatus.BAD_GATEWAY, HTTPStatus.SERVICE_UNAVAILABLE, HTTPStatus.GATEWAY_TIMEOUT):
                if attempt == max_retries:
                    resp.raise_for_status()
                time.sleep(backoff)
                backoff *= 2
                continue
            resp.raise_for_status()
            return resp.json()

    # Shouldn't reach here because raise_for_status above will have thrown.
    raise RuntimeError("Unreachable: _get_with_retry loop exhausted without raising")


# ---------- channel lookup functions ----------

def get_channel_id_by_handle(handle: str) -> Optional[str]:
    """
    Get channel ID from handle using channels.list?forHandle.
    Handles should include @ symbol (e.g., '@GoogleDevelopers').
    Returns None if handle not found.
    """
    # Remove @ if user included it
    clean_handle = handle.lstrip('@')
    
    params = {
        "part": "id",
        "forHandle": f"@{clean_handle}",
    }
    
    try:
        data = _get_with_retry("channels", params)
        items = data.get("items", [])
        if items:
            return items[0].get("id")
        return None
    except Exception:
        # Handle not found or other error
        return None

def get_channel_info_by_handle(handle: str) -> Optional[Dict[str, Any]]:
    """
    Get full channel information from handle.
    Returns None if handle not found.
    """
    clean_handle = handle.lstrip('@')
    
    params = {
        "part": "snippet,statistics,contentDetails",
        "forHandle": f"@{clean_handle}",
    }
    
    try:
        data = _get_with_retry("channels", params)
        items = data.get("items", [])
        if items:
            return items[0]
        return None
    except Exception:
        return None


# ---------- public API ----------

def get_most_popular(region: str, max_pages: int = 2) -> List[Dict[str, Any]]:
    """
    videos.list?chart=mostPopular for region.
    Paginates using nextPageToken.
    """
    items: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    pages = 0

    while pages < max_pages:
        params = {
            "chart": "mostPopular",
            "part": "snippet,statistics,contentDetails,topicDetails",
            "regionCode": region.upper(),
            "maxResults": 50,
        }
        if page_token:
            params["pageToken"] = page_token

        data = _get_with_retry("videos", params)
        batch = data.get("items", [])
        if not batch:
            break
        items.extend(batch)

        page_token = data.get("nextPageToken")
        pages += 1
        if not page_token:
            break

    return items


def search_video_ids(query: str,
                     relevance_language: Optional[str],
                     published_after_iso: Optional[str],
                     max_pages: int = 10) -> List[str]:
    """
    search.list(type=video) → collect video IDs across pages.
    """
    ids: List[str] = []
    seen = set()
    page_token: Optional[str] = None
    pages = 0

    while pages < max_pages:
        params = {
            "part": "snippet",
            "type": "video",
            "order": "date",
            "maxResults": 50,
            "q": query,
        }
        if relevance_language:
            params["relevanceLanguage"] = relevance_language
        if published_after_iso:
            params["publishedAfter"] = published_after_iso
        if page_token:
            params["pageToken"] = page_token

        data = _get_with_retry("search", params)
        batch = data.get("items", [])
        if not batch:
            break

        for item in batch:
            vid = item.get("id", {}).get("videoId")
            if vid and vid not in seen:
                seen.add(vid)
                ids.append(vid)

        page_token = data.get("nextPageToken")
        pages += 1
        if not page_token:
            break

    return ids


def hydrate_videos(video_ids: List[str]) -> List[Dict[str, Any]]:
    """
    videos.list (chunked by 50) → full resources.
    """
    if not video_ids:
        return []

    out: List[Dict[str, Any]] = []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i + 50]
        params = {
            "part": "snippet,statistics,contentDetails,topicDetails",
            "id": ",".join(chunk),
            "maxResults": 50,
        }
        data = _get_with_retry("videos", params)
        out.extend(data.get("items", []))
    return out


def list_channel_upload_ids(channel_id: str, max_pages: int = 10) -> List[str]:
    """
    Preferred method: use uploads playlist from channels.contentDetails,
    then page through playlistItems to get video IDs in reverse-chron order.
    """
    # 1) channels.list to get uploads playlistId
    ch = _get_with_retry("channels", {
        "part": "contentDetails",
        "id": channel_id
    })
    items = ch.get("items", [])
    if not items:
        return []
    uploads_playlist = items[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
    if not uploads_playlist:
        return []

    # 2) playlistItems.list to page through uploads
    ids: List[str] = []
    seen = set()
    page_token: Optional[str] = None
    pages = 0

    while pages < max_pages:
        params = {
            "part": "contentDetails",
            "playlistId": uploads_playlist,
            "maxResults": 50,
        }
        if page_token:
            params["pageToken"] = page_token

        data = _get_with_retry("playlistItems", params)
        batch = data.get("items", [])
        if not batch:
            break

        for it in batch:
            vid = it.get("contentDetails", {}).get("videoId")
            if vid and vid not in seen:
                seen.add(vid)
                ids.append(vid)

        page_token = data.get("nextPageToken")
        pages += 1
        if not page_token:
            break

    return ids


def list_channel_upload_ids_by_handle(handle: str, max_pages: int = 10) -> List[str]:
    """
    Convenience function: handle → channel ID → upload IDs.
    """
    channel_id = get_channel_id_by_handle(handle)
    if not channel_id:
        raise ValueError(f"Channel not found for handle: {handle}")
    
    return list_channel_upload_ids(channel_id, max_pages)


def extract_video_id(video: Dict[str, Any]) -> str:
    """
    Robust extractor: works for both hydrated videos.list items (id is str)
    and raw search.list items (id is dict with videoId).
    """
    vid = video.get("id")
    if isinstance(vid, dict):
        return vid.get("videoId", "")
    return vid or ""


# ---------- batch operations for multiple channels ----------

def get_multiple_channel_upload_ids(handles: List[str], max_pages_per_channel: int = 10) -> Dict[str, List[str]]:
    """
    Get upload IDs for multiple channels by handle.
    Returns dict mapping handle to list of video IDs.
    """
    results = {}
    for handle in handles:
        try:
            upload_ids = list_channel_upload_ids_by_handle(handle, max_pages_per_channel)
            results[handle] = upload_ids
            print(f"✓ Found {len(upload_ids)} videos for {handle}")
        except Exception as e:
            print(f"✗ Error getting videos for {handle}: {e}")
            results[handle] = []
    
    return results


def hydrate_multiple_channels(handles: List[str], max_pages_per_channel: int = 10) -> Dict[str, List[Dict[str, Any]]]:
    """
    Get full video data for multiple channels by handle.
    Returns dict mapping handle to list of video objects.
    """
    # First get all video IDs
    upload_ids_by_channel = get_multiple_channel_upload_ids(handles, max_pages_per_channel)
    
    # Then hydrate all videos
    results = {}
    for handle, video_ids in upload_ids_by_channel.items():
        if video_ids:
            videos = hydrate_videos(video_ids)
            results[handle] = videos
            print(f"✓ Hydrated {len(videos)} videos for {handle}")
        else:
            results[handle] = []
    
    return results


# ---------- channel enrichment for videos ----------

def _fetch_channels_info(channel_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch channel info (snippet, statistics) for a list of channel IDs.
    Returns a dict mapping channelId -> channel resource.
    """
    if not channel_ids:
        return {}

    unique_ids = list({cid for cid in channel_ids if cid})
    result: Dict[str, Dict[str, Any]] = {}

    for i in range(0, len(unique_ids), 50):
        chunk = unique_ids[i:i + 50]
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(chunk),
            "maxResults": 50,
        }
        data = _get_with_retry("channels", params)
        for item in data.get("items", []):
            cid = item.get("id")
            if cid:
                result[cid] = item

    return result


def attach_channel_stats(videos: List[Dict[str, Any]]) -> None:
    """
    Enrich each video dict with channel statistics under `_channel_metadata`.
    Adds:
      - subscriberCount (int, if visible)
      - videoCount (int)
      - hiddenSubscriberCount (bool)
      - channelId (str)
    Modifies the input list in place. Safe to call with empty list.
    """
    if not videos:
        return

    channel_ids: List[str] = []
    for v in videos:
        ch_id = v.get("snippet", {}).get("channelId")
        if ch_id:
            channel_ids.append(ch_id)

    info_by_id = _fetch_channels_info(channel_ids)

    for v in videos:
        ch_id = v.get("snippet", {}).get("channelId")
        ch = info_by_id.get(ch_id)
        if not ch:
            continue
        stats = ch.get("statistics", {}) or {}
        subscriber_count_raw = stats.get("subscriberCount")
        video_count_raw = stats.get("videoCount")
        hidden_flag = stats.get("hiddenSubscriberCount")
        try:
            subscriber_count = int(subscriber_count_raw) if subscriber_count_raw is not None else None
        except Exception:
            subscriber_count = None
        try:
            video_count = int(video_count_raw) if video_count_raw is not None else None
        except Exception:
            video_count = None

        v["_channel_metadata"] = {
            "channelId": ch_id,
            "subscriberCount": subscriber_count,
            "videoCount": video_count,
            "hiddenSubscriberCount": bool(hidden_flag) if hidden_flag is not None else None,
        }

# ---------- quota management utilities ----------

def get_quota_manager() -> YoutubeQuotaManager:
    """Get the global quota manager instance."""
    return _quota_manager

def check_quota_status() -> Dict[str, Any]:
    """Get current quota status for monitoring."""
    return {
        "remaining": _quota_manager.remaining(),
        "used": _quota_manager.used(),
        "daily_cap": _quota_manager._daily_cap,
        "will_reset_at": datetime.fromtimestamp(_quota_manager.quota_reset_time, _quota_manager._tz).isoformat()
    }

def estimate_quota_usage(programs: List[str],
                         keywords: List[str] = None,
                         channel_handles: List[str] = None,
                         regions: List[str] = None,
                         max_pages: int = 2) -> Dict[str, Any]:
    """
    Estimate quota usage for the planned pipeline run.
    
    Returns:
        Dict with estimated costs and recommendations
    """
    total_estimated = 0
    breakdown = {}
    
    if 'keywords' in programs and keywords:
        # search.list costs 100 units per page
        cost = len(keywords) * max_pages * 100
        breakdown['keywords'] = {
            'cost': cost,
            'description': f"{len(keywords)} keywords × {max_pages} pages × 100 units"
        }
        total_estimated += cost
    
    if 'competitors' in programs and channel_handles:
        # channels.list (1) + playlistItems.list (1) per page per channel
        cost = len(channel_handles) * (1 + max_pages)
        breakdown['competitors'] = {
            'cost': cost,
            'description': f"{len(channel_handles)} channels × (1 + {max_pages} pages) units"
        }
        total_estimated += cost
    
    if 'trending' in programs and regions:
        # videos.list costs 1 unit per page per region
        cost = len(regions) * max_pages
        breakdown['trending'] = {
            'cost': cost,
            'description': f"{len(regions)} regions × {max_pages} pages × 1 unit"
        }
        total_estimated += cost
    
    # Hydration costs (1 unit per 50 videos)
    estimated_videos = 0
    if 'keywords' in programs and keywords:
        estimated_videos += len(keywords) * max_pages * 50  # Rough estimate
    if 'competitors' in programs and channel_handles:
        estimated_videos += len(channel_handles) * max_pages * 50
    if 'trending' in programs and regions:
        estimated_videos += len(regions) * max_pages * 50
    
    hydration_cost = max(1, estimated_videos // 50)  # 1 unit per 50 videos
    breakdown['hydration'] = {
        'cost': hydration_cost,
        'description': f"~{estimated_videos} videos ÷ 50 = {hydration_cost} units"
    }
    total_estimated += hydration_cost
    
    return {
        'total_estimated': total_estimated,
        'breakdown': breakdown,
        'will_fit': _quota_manager.remaining() >= total_estimated,
        'remaining': _quota_manager.remaining(),
        'recommendation': '✅ Safe to run' if _quota_manager.remaining() >= total_estimated else '⚠️ Consider reducing max_pages'
    }