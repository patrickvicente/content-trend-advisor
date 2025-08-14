#!/usr/bin/env python3
"""
Backfill `_channel_metadata` for existing YouTube rows in Postgres.

- Finds distinct channelIds where payload._channel_metadata is NULL
- Calls YouTube channels.list (batched) to fetch statistics
- Updates payload JSONB with `_channel_metadata` for all rows of that channel

Env required:
- DATABASE_URL
- YOUTUBE_API_KEY (used by services.etl.youtube_client)

Usage:
  python infra/scripts/backfill_channel_metadata.py --batch-size 50 --limit-channels 1000
"""

from __future__ import annotations

import argparse
import json
import os
from typing import List

import psycopg

# Reuse channel fetch from our client module
from services.etl.youtube_client import _fetch_channels_info


def to_int(value):
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def backfill(batch_size: int, limit_channels: int | None = None) -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set")

    # Collect distinct channel IDs missing metadata
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT payload->'snippet'->>'channelId' AS channel_id
            FROM public.raw_content
            WHERE source='youtube'
              AND payload->'_channel_metadata' IS NULL
              AND payload->'snippet'->>'channelId' IS NOT NULL
            """
        )
        channel_ids: List[str] = [r[0] for r in cur.fetchall()]

    if limit_channels is not None:
        channel_ids = channel_ids[: int(limit_channels)]

    total_channels = len(channel_ids)
    print(f"Channels to backfill: {total_channels}")
    if total_channels == 0:
        return

    updated_rows = 0
    with psycopg.connect(dsn) as conn:
        for i in range(0, total_channels, batch_size):
            chunk = channel_ids[i : i + batch_size]
            info_by_id = _fetch_channels_info(chunk)

            if not info_by_id:
                print(f"{i+len(chunk)}/{total_channels}: no info returned")
                continue

            with conn.cursor() as cur:
                for cid, ch in info_by_id.items():
                    stats = (ch.get("statistics") or {})
                    md = {
                        "channelId": cid,
                        "subscriberCount": to_int(stats.get("subscriberCount")),
                        "videoCount": to_int(stats.get("videoCount")),
                        "hiddenSubscriberCount": bool(stats.get("hiddenSubscriberCount"))
                        if stats.get("hiddenSubscriberCount") is not None
                        else None,
                    }

                    cur.execute(
                        """
                        UPDATE public.raw_content
                        SET payload = jsonb_set(payload, '{_channel_metadata}', %s::jsonb, true)
                        WHERE source='youtube'
                          AND payload->'snippet'->>'channelId' = %s
                          AND payload->'_channel_metadata' IS NULL
                        """,
                        (json.dumps(md), cid),
                    )
                    updated_rows += cur.rowcount
            conn.commit()
            print(
                f"Processed {i+len(chunk)}/{total_channels} channels; rows updated so far: {updated_rows}"
            )

    print(f"Backfill complete. Rows updated: {updated_rows}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--limit-channels", type=int, default=None)
    args = parser.parse_args()

    backfill(batch_size=args.batch_size, limit_channels=args.limit_channels)


if __name__ == "__main__":
    main()


