import os
import time
from http import HTTPStatus
from typing import List, Dict, Any, Optional

import requests

BASE = "https://www.googleapis.com/youtube/v3"

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
    api_key = _get_api_key()  # Get API key dynamically
    params = {**params, "key": api_key}
    backoff = 2
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

    # Shouldn’t reach here because raise_for_status above will have thrown.
    raise RuntimeError("Unreachable: _get_with_retry loop exhausted without raising")


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


def extract_video_id(video: Dict[str, Any]) -> str:
    """
    Robust extractor: works for both hydrated videos.list items (id is str)
    and raw search.list items (id is dict with videoId).
    """
    vid = video.get("id")
    if isinstance(vid, dict):
        return vid.get("videoId", "")
    return vid or ""