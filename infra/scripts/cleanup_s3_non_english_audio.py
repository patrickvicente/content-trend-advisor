#!/usr/bin/env python3
"""
Archive+delete non-English-audio YouTube raw objects from S3/MinIO.

Policy:
  - Keep defaultAudioLanguage: NULL, 'zxx', or starting with 'en'
  - Archive others from raw/youtube/ → archive/raw/youtube/

Run:
  DRY RUN: python infra/scripts/cleanup_s3_non_english_audio.py --dry-run --limit 500
  EXECUTE: python infra/scripts/cleanup_s3_non_english_audio.py
"""

import os
import json
import argparse
import boto3


def is_english_audio(lang: str | None) -> bool:
    if not lang:
        return True
    lang = str(lang).lower()
    return lang == "zxx" or lang.startswith("en")


def iter_keys(client, bucket, prefix):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="List and summarize only")
    parser.add_argument("--limit", type=int, default=None, help="Limit keys to scan for testing")
    args = parser.parse_args()

    endpoint = os.getenv("S3_ENDPOINT")
    access = os.getenv("AWS_ACCESS_KEY_ID")
    secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket = os.getenv("S3_BUCKET", "content")

    if not all([endpoint, access, secret, bucket]):
        raise SystemExit("Missing S3 env vars: S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )

    source_prefix = "raw/youtube/"
    archive_prefix = "archive/raw/youtube/"

    total_scanned = 0
    candidates = []
    for i, key in enumerate(iter_keys(s3, bucket, source_prefix), 1):
        if args.limit and i > args.limit:
            break
        total_scanned = i
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        try:
            payload = json.loads(body)
        except Exception:
            continue
        lang = payload.get("snippet", {}).get("defaultAudioLanguage")
        if not is_english_audio(lang):
            candidates.append((key, lang))

    print(f"Scanned: {total_scanned}, Non-English audio: {len(candidates)}")
    if args.dry_run:
        for k, l in candidates[:10]:
            print(f"DRY-RUN example: {k} defaultAudioLanguage={l}")
        return

    # Execute archive + delete
    for key, lang in candidates:
        archive_key = key.replace(source_prefix, archive_prefix, 1)
        s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=archive_key, MetadataDirective="COPY")
        s3.delete_object(Bucket=bucket, Key=key)
    print("Archive+delete complete")


if __name__ == "__main__":
    main()


