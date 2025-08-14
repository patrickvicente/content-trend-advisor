from typing import List, Dict
import os
import json
from datetime import datetime, timezone

from .dbio import get_conn
from .youtube_client import hydrate_videos


def snapshot_stats(video_ids: List[str]) -> int:
    """
    Hydrate current metrics for the given YouTube video IDs and append a row
    in raw_metrics_snapshots for each. Returns number of rows inserted.
    """
    if not video_ids:
        return 0

    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("DATABASE_URL environment variable not set")

    videos = hydrate_videos(video_ids)

    inserted = 0
    with get_conn(dsn) as conn, conn.cursor() as cur:
        for v in videos:
            stats = v.get("statistics", {}) or {}
            vid = v.get("id")
            try:
                view_count = int(stats.get("viewCount")) if stats.get("viewCount") is not None else None
                like_count = int(stats.get("likeCount")) if stats.get("likeCount") is not None else None
                comment_count = int(stats.get("commentCount")) if stats.get("commentCount") is not None else None
            except Exception:
                view_count = like_count = comment_count = None

            cur.execute(
                """
                INSERT INTO public.raw_metrics_snapshots (source, external_id, fetched_at, view_count, like_count, comment_count)
                VALUES (%s, %s, NOW(), %s, %s, %s)
                """,
                ("youtube", vid, view_count, like_count, comment_count)
            )
            inserted += 1
        conn.commit()

    return inserted


def compute_view_deltas(conn, horizon_hours: int = 24) -> List[Dict]:
    """
    Compute 24h/48h deltas from raw_metrics_snapshots for each video.
    Returns rows with external_id, delta_views_{horizon}h.
    """
    results: List[Dict] = []
    with conn.cursor() as cur:
        sql = f"""
            WITH latest AS (
                SELECT source, external_id,
                       max(fetched_at) AS latest_at
                FROM public.raw_metrics_snapshots
                WHERE source = 'youtube'
                GROUP BY 1,2
            ),
            base AS (
                SELECT s.source, s.external_id,
                       s.view_count AS latest_views,
                       s.fetched_at AS latest_at
                FROM public.raw_metrics_snapshots s
                JOIN latest l ON l.source = s.source AND l.external_id = s.external_id
                WHERE s.fetched_at = l.latest_at
            ),
            prior_candidates AS (
                SELECT s.source, s.external_id,
                       s.view_count AS prior_views,
                       s.fetched_at,
                       ROW_NUMBER() OVER (
                         PARTITION BY s.source, s.external_id
                         ORDER BY s.fetched_at DESC
                       ) AS rn
                FROM public.raw_metrics_snapshots s
                JOIN latest l ON l.source = s.source AND l.external_id = s.external_id
                WHERE s.fetched_at <= l.latest_at - INTERVAL '{horizon_hours} hours'
            ),
            prior AS (
                SELECT source, external_id, prior_views
                FROM prior_candidates
                WHERE rn = 1
            )
            SELECT b.external_id,
                   (b.latest_views - COALESCE(p.prior_views, b.latest_views)) AS delta_views_{horizon_hours}h
            FROM base b
            LEFT JOIN prior p ON p.source = b.source AND p.external_id = b.external_id
        """
        cur.execute(sql)
        for row in cur.fetchall():
            results.append({
                "external_id": row[0],
                f"delta_views_{horizon_hours}h": row[1]
            })
    return results