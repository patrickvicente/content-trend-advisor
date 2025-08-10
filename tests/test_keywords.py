#!/usr/bin/env python3
"""
Test script for YouTube ingestion keywords program.
Quick testing without complex environment setup.
"""
import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Load environment variables from .env file
def load_env_file():
    """Load environment variables from .env file if it exists."""
    env_file = Path(project_root) / '.env'
    if env_file.exists():
        try:
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        # Remove quotes if present
                        value = value.strip('"\'')
                        os.environ[key] = value
            print(f"✅ Loaded environment variables from .env")
        except Exception as e:
            print(f"⚠️  Warning: Could not load .env file: {e}")

# Load .env on import
load_env_file()

from services.etl.youtube_ingest import run_keywords_program, apply_relevance_filters
from services.etl.filters import filter_content

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def test_keywords_dry_run():
    """
    Test keywords program without persistence (dry run).
    """
    print("🧪 Testing YouTube Keywords Program (Dry Run)")
    print("=" * 50)
    
    # Test configuration
    test_keywords = [
        "N8N automation tutorial",
        "no code workflow automation", 
        "AI programming tools",
        "Python automation scripts"
    ]
    
    test_config = {
        "queries": test_keywords,
        "relevance_lang": ["en"],
        "published_after_iso": None,  # Get recent content
        "max_pages": 1  # Small test
    }
    
    print(f"🔍 Testing with keywords: {test_keywords}")
    print(f"🌐 Languages: {test_config['relevance_lang']}")
    print(f"📄 Max pages per query: {test_config['max_pages']}")
    print()
    
    try:
        # Step 1: Fetch videos (requires YOUTUBE_API_KEY)
        print("Step 1: Fetching videos from YouTube...")
        if not os.getenv("YOUTUBE_API_KEY"):
            print("❌ YOUTUBE_API_KEY not set. Cannot test actual API calls.")
            print("💡 Set YOUTUBE_API_KEY environment variable to test API integration.")
            return test_filtering_only()
        
        videos = run_keywords_program(**test_config)
        print(f"✅ Fetched {len(videos)} videos")
        
        if not videos:
            print("ℹ️  No videos found. This could be normal for very specific keywords.")
            return
        
        # Step 2: Show sample video data
        print("\n📋 Sample Video Data:")
        for i, video in enumerate(videos[:2]):  # Show first 2
            snippet = video.get('snippet', {})
            print(f"  {i+1}. {snippet.get('title', 'No title')}")
            print(f"     Channel: {snippet.get('channelTitle', 'Unknown')}")
            print(f"     Category: {snippet.get('categoryId', 'Unknown')}")
            print()
        
        # Step 3: Test filtering
        print("Step 2: Testing relevance filters...")
        test_filtering(videos)
        
    except Exception as e:
        logger.error(f"Error in test: {e}")
        print(f"❌ Test failed: {e}")

def test_filtering_only():
    """
    Test just the filtering logic with mock data.
    """
    print("\n🔬 Testing Filtering Logic (Mock Data)")
    print("=" * 40)
    
    # Mock video data for testing filters
    mock_videos = [
        {
            "id": "test1",
            "snippet": {
                "title": "Complete N8N Automation Tutorial - No Code Workflows",
                "description": "Learn how to build powerful automations with N8N without writing code...",
                "categoryId": "28",  # Science & Technology
                "channelTitle": "Automation Channel",
                "defaultAudioLanguage": "en"
            }
        },
        {
            "id": "test2", 
            "snippet": {
                "title": "Building AI Apps with Python and FastAPI",
                "description": "Tutorial on creating machine learning applications using Python...",
                "categoryId": "28",  # Science & Technology
                "channelTitle": "AI Tutorials",
                "defaultAudioLanguage": "en"
            }
        },
        {
            "id": "test3",
            "snippet": {
                "title": "Cooking Pasta - Italian Style",
                "description": "Traditional Italian pasta cooking techniques...",
                "categoryId": "26",  # Howto & Style (cooking)
                "channelTitle": "Cooking Channel",
                "defaultAudioLanguage": "en"
            }
        }
    ]
    
    print(f"📊 Testing with {len(mock_videos)} mock videos")
    test_filtering(mock_videos)

def test_filtering(videos):
    """Test the filtering pipeline."""
    print(f"\n🔍 Testing filters on {len(videos)} videos...")
    
    # Test individual video filtering
    for i, video in enumerate(videos[:3]):  # Test first 3
        snippet = video.get('snippet', {})
        title = snippet.get('title', '')
        description = snippet.get('description', '')
        category_id = snippet.get('categoryId')
        
        print(f"\n📹 Video {i+1}: {title[:50]}...")
        
        # Test filter_content function
        filter_result = filter_content(
            title=title,
            description=description,
            category_id=category_id,
            allowed_languages=["en"],
            allowed_categories=["Science & Technology", "Education", "Howto & Style"]
        )
        
        print(f"   🌐 Language: {filter_result['language']} ({'✅' if filter_result['language_ok'] else '❌'})")
        print(f"   📂 Category: {filter_result['category']} ({'✅' if filter_result['category_ok'] else '❌'})")
        print(f"   🏷️  Topics: {filter_result['topics']} ({'✅' if filter_result['topics_ok'] else '❌'})")
        print(f"   🎯 Overall: {'✅ PASS' if filter_result['is_allowed'] else '❌ FILTERED'}")
    
    # Test batch filtering
    print(f"\n📊 Batch Filtering Results:")
    filtered_videos = apply_relevance_filters(
        videos=videos,
        allowed_languages=["en"],
        allowed_categories=["Science & Technology", "Education", "Howto & Style"]
    )
    
    pass_rate = len(filtered_videos) / len(videos) * 100 if videos else 0
    print(f"   📈 Pass rate: {len(filtered_videos)}/{len(videos)} ({pass_rate:.1f}%)")
    
    if filtered_videos:
        print("   ✅ Sample passing videos:")
        for video in filtered_videos[:2]:
            title = video.get('snippet', {}).get('title', 'No title')
            topics = video.get('_filter_metadata', {}).get('detected_topics', [])
            print(f"      • {title[:60]}...")
            print(f"        Topics: {', '.join(topics)}")

def test_environment():
    """Check environment setup."""
    print("🔧 Environment Check")
    print("=" * 20)
    
    required_vars = ["YOUTUBE_API_KEY"]
    optional_vars = ["DATABASE_URL", "S3_BUCKET", "S3_ENDPOINT"]
    
    for var in required_vars:
        value = os.getenv(var)
        status = "✅ SET" if value else "❌ MISSING"
        print(f"{var}: {status}")
    
    print("\nOptional (for full pipeline):")
    for var in optional_vars:
        value = os.getenv(var)
        status = "✅ SET" if value else "⚠️  NOT SET"
        print(f"{var}: {status}")
    
    print()

if __name__ == "__main__":
    print("🎯 YouTube Keywords Program Test Suite")
    print("=====================================")
    
    # Check environment
    test_environment()
    
    # Run tests
    test_keywords_dry_run()
    
    print("\n🎉 Test completed!")
    print("\n💡 Next steps:")
    print("  1. Set YOUTUBE_API_KEY to test real API calls")
    print("  2. Set DATABASE_URL and S3_BUCKET for full pipeline testing")
    print("  3. Run: python test_keywords.py")
