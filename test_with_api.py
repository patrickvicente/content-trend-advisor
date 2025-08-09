#!/usr/bin/env python3
"""
Test YouTube ingestion with actual API calls.
Requires YOUTUBE_API_KEY environment variable.
"""
import os
import sys
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
            print(f"✅ Loaded environment variables from {env_file}")
        except Exception as e:
            print(f"⚠️  Warning: Could not load .env file: {e}")
    else:
        print(f"ℹ️  No .env file found at {env_file}")

# Load .env on import
load_env_file()

def test_with_youtube_api():
    """Test keywords program with real YouTube API."""
    
    # Check for API key
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        print("❌ YOUTUBE_API_KEY not set!")
        print("💡 To test with real data:")
        print("   1. Get YouTube API key from Google Cloud Console")
        print("   2. export YOUTUBE_API_KEY='your_key_here'") 
        print("   3. python test_with_api.py")
        return
    
    try:
        from services.etl.youtube_ingest import run_keywords_program, apply_relevance_filters
        
        print("🧪 Testing YouTube API Integration")
        print("=" * 40)
        
        # Test configuration - small scale
        test_config = {
            "queries": ["N8N automation", "AI programming"],  # Your niche keywords
            "relevance_lang": ["en"],
            "published_after_iso": None,
            "max_pages": 1  # Small test
        }
        
        print(f"🔍 Testing keywords: {test_config['queries']}")
        print("🚀 Fetching from YouTube API...")
        
        # Fetch real videos
        videos = run_keywords_program(**test_config)
        
        print(f"✅ Fetched {len(videos)} videos from YouTube")
        
        if videos:
            # Show sample data
            print("\n📋 Sample Videos:")
            for i, video in enumerate(videos[:3]):
                snippet = video.get('snippet', {})
                stats = video.get('statistics', {})
                print(f"\n{i+1}. {snippet.get('title', 'No title')}")
                print(f"   Channel: {snippet.get('channelTitle', 'Unknown')}")
                print(f"   Views: {stats.get('viewCount', 'N/A')}")
                print(f"   Category: {snippet.get('categoryId', 'Unknown')}")
            
            # Apply filters
            print(f"\n🔍 Applying filters to {len(videos)} videos...")
            filtered = apply_relevance_filters(
                videos=videos,
                allowed_languages=["en"],
                allowed_categories=["Science & Technology", "Education", "Howto & Style"]
            )
            
            print(f"\n📊 Results:")
            print(f"   Raw videos: {len(videos)}")
            print(f"   After filters: {len(filtered)}")
            print(f"   Pass rate: {len(filtered)/len(videos)*100:.1f}%")
            
            if filtered:
                print("\n✅ Filtered videos (passing):")
                for video in filtered[:2]:
                    snippet = video.get('snippet', {})
                    metadata = video.get('_filter_metadata', {})
                    print(f"   • {snippet.get('title', 'No title')}")
                    print(f"     Topics: {', '.join(metadata.get('detected_topics', []))}")
        else:
            print("⚠️  No videos found. Try different keywords or check API quota.")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

def test_simple_search():
    """Simple search test without full pipeline."""
    api_key = os.getenv("YOUTUBE_API_KEY") 
    if not api_key:
        print("❌ Need YOUTUBE_API_KEY for this test")
        return
        
    try:
        from services.etl.youtube_client import search_video_ids, hydrate_videos
        
        print("\n🔎 Simple Search Test")
        print("=" * 25)
        
        # Search for N8N videos
        print("Searching for 'N8N automation'...")
        video_ids = search_video_ids("N8N automation", "en", None, 1)
        
        print(f"Found {len(video_ids)} video IDs: {video_ids[:3]}")
        
        if video_ids:
            print("Hydrating video details...")
            videos = hydrate_videos(video_ids[:3])  # Just first 3
            
            for video in videos:
                snippet = video.get('snippet', {})
                print(f"📹 {snippet.get('title', 'No title')}")
                print(f"   Channel: {snippet.get('channelTitle', 'Unknown')}")
                
    except Exception as e:
        print(f"❌ Search test error: {e}")

if __name__ == "__main__":
    print("🎯 YouTube API Integration Test")
    print("==============================")
    
    test_with_youtube_api()
    test_simple_search()
    
    print("\n🎉 API tests completed!")
    print("\n💡 To test full pipeline:")
    print("   export YOUTUBE_API_KEY='your_key'")
    print("   export DATABASE_URL='postgresql://...'")
    print("   export S3_BUCKET='content'")
    print("   python -m services.etl.youtube_ingest --programs keywords --max-pages 1")
