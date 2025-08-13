#!/usr/bin/env python3
"""
Minimal YouTube API test to validate connectivity and basic functionality.
This script uses minimal API calls to test the pipeline without wasting tokens.
"""

import sys
import os
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import our modules
from services.etl.youtube_client import search_video_ids, hydrate_videos
from services.etl.filters import filter_youtube_video

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_env_file():
    """Load environment variables from .env file."""
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        logger.info("✅ Loaded .env file")
    else:
        logger.warning("⚠️ .env file not found")

def test_api_connection():
    """Test basic YouTube API connection."""
    logger.info("🔌 Testing YouTube API connection...")
    
    try:
        logger.info("✅ YouTube client functions imported")
        
        # Test with a single, specific search to minimize token usage
        video_ids = search_video_ids(
            query="N8N automation",
            relevance_language="en",
            published_after_iso=None,
            max_pages=1  # Only 1 page to save tokens
        )
        
        if video_ids:
            logger.info(f"✅ Search successful, found {len(video_ids)} videos")
            
            # Test hydration with just 1 video
            video_id = video_ids[0]
            logger.info(f"🔍 Testing hydration for video: {video_id}")
            
            hydrated_video = hydrate_videos([video_id])
            
            if hydrated_video:
                logger.info(f"✅ Hydration successful for video: {video_id}")
                
                # Test filtering with real data
                video_data = hydrated_video[0]
                filter_result = filter_youtube_video(video_data, "youtube")
                
                logger.info(f"✅ Filtering successful:")
                logger.info(f"   Title: {video_data['snippet']['title']}")
                logger.info(f"   Language: {filter_result.get('language', 'unknown')}")
                logger.info(f"   Category: {filter_result.get('category_name', 'unknown')}")
                logger.info(f"   Relevant: {filter_result.get('is_relevant', False)}")
                logger.info(f"   Topics: {filter_result.get('topic_labels', [])}")
                
                return True
            else:
                logger.error("❌ Video hydration failed")
                return False
        else:
            logger.warning("⚠️ No search results found")
            return False
            
    except Exception as e:
        logger.error(f"❌ API test failed: {e}")
        return False

def test_database_connection():
    """Test database connection without inserting data."""
    logger.info("🗄️ Testing database connection...")
    
    try:
        from services.etl.dbio import get_conn
        
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            logger.warning("⚠️ DATABASE_URL not set")
            return False
        
        conn = get_conn(dsn)
        logger.info("✅ Database connection successful")
        
        # Test a simple query
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM raw_content")
            count = cur.fetchone()[0]
            logger.info(f"✅ Database query successful, current row count: {count}")
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Database test failed: {e}")
        return False

def main():
    """Run minimal tests."""
    logger.info("🚀 Starting minimal API testing...")
    
    # Load environment
    load_env_file()
    
    # Test database first (no API tokens needed)
    db_ok = test_database_connection()
    print()
    
    if db_ok:
        # Test API (minimal token usage)
        api_ok = test_api_connection()
        print()
        
        if api_ok:
            logger.info("🎉 All minimal tests passed! Your pipeline is ready.")
            logger.info("💡 You can now run the full pipeline with confidence.")
        else:
            logger.error("❌ API test failed. Check your API key and quota.")
    else:
        logger.error("❌ Database test failed. Check your database connection.")

if __name__ == "__main__":
    main()
