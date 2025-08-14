#!/usr/bin/env python3
"""
Unit tests for strict English-only gating:
- filter_youtube_video enforces audio-language gate
- apply_relevance_filters propagates audio-language metadata
"""

import os
import sys
from pathlib import Path

# Ensure project root on path (tests/ parent)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from services.etl.filters import filter_youtube_video
from services.etl.youtube_ingest import apply_relevance_filters


def _make_video(title: str, description: str = "", category_id: str = "28", default_audio_language=None):
    return {
        "id": "vid_" + title.replace(" ", "_")[:20],
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": category_id,
            "defaultAudioLanguage": default_audio_language,
        },
        "statistics": {
            "viewCount": "1234",
            "likeCount": "56",
            "commentCount": "7",
        },
    }


def test_filter_youtube_video_audio_language_gate():
    # Text clearly English and on-topic (AI/Automation/Programming all required topics)
    base_title = "AI automation programming tutorial"
    description = "Learn AI and automation programming with Python"

    # Case 1: Non-English audio -> should be filtered out
    v_hi = _make_video(base_title, description, default_audio_language="hi")
    r_hi = filter_youtube_video(v_hi, "youtube")
    assert r_hi["is_relevant"] is False
    assert r_hi["filter_metadata"]["default_audio_language"] == "hi"
    assert r_hi["filter_metadata"]["audio_language_ok"] is False

    # Case 2: English audio -> should pass
    v_en = _make_video(base_title, description, default_audio_language="en")
    r_en = filter_youtube_video(v_en, "youtube")
    assert r_en["is_relevant"] is True
    assert r_en["filter_metadata"]["audio_language_ok"] is True

    # Case 3: English locale -> should pass
    v_en_us = _make_video(base_title, description, default_audio_language="en-US")
    r_en_us = filter_youtube_video(v_en_us, "youtube")
    assert r_en_us["is_relevant"] is True
    assert r_en_us["filter_metadata"]["audio_language_ok"] is True

    # Case 4: Missing audio language -> treated as OK
    v_none = _make_video(base_title, description, default_audio_language=None)
    r_none = filter_youtube_video(v_none, "youtube")
    assert r_none["is_relevant"] is True
    assert r_none["filter_metadata"]["audio_language_ok"] is True

    # Case 5: zxx (no linguistic content) -> treated as OK
    v_zxx = _make_video(base_title, description, default_audio_language="zxx")
    r_zxx = filter_youtube_video(v_zxx, "youtube")
    assert r_zxx["is_relevant"] is True
    assert r_zxx["filter_metadata"]["audio_language_ok"] is True


def test_apply_relevance_filters_propagates_audio_metadata():
    videos = [
        _make_video("AI programming automation", "on-topic", default_audio_language="en"),
        _make_video("AI programming automation", "on-topic", default_audio_language="fa"),
    ]

    filtered = apply_relevance_filters(
        videos=videos,
        allowed_languages=["en"],
        allowed_categories=["Science & Technology"],
        denied_categories=[],
    )

    # Only the English-audio video should pass
    assert len(filtered) == 1
    kept = filtered[0]
    fm = kept.get("_filter_metadata", {})
    assert fm.get("detected_language", "").startswith("en")
    assert fm.get("audio_language_ok") is True
    # defaultAudioLanguage propagated
    assert fm.get("default_audio_language") in ("en", "en-US", None, "zxx") or fm.get("default_audio_language").startswith("en")


