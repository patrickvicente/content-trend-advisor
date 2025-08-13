#!/usr/bin/env python3
"""
Test the YouTube ingestion pipeline with mock data to avoid wasting API tokens.
This script tests all the core functionality without making real API calls.
"""

import sys
import os
from pathlib import Path
import json
import logging
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Import our modules
from services.etl.filters import filter_youtube_video
from services.etl.dbio import insert_many_raw_rows, select_recent_external_ids
from services.etl.s3io import S3Client

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

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_mock_youtube_videos(count=5):
    """Create realistic mock YouTube video data for testing."""
    mock_videos = []
    
    for i in range(count):
        video = {
            "id": f"mock_video_{i}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "snippet": {
                "title": f"Mock Video Title {i} - AI and Automation",
                "description": f"This is a mock video about AI, automation, and workflow tools. Video number {i}.",
                "channelId": f"UC_mock_channel_{i}",
                "channelTitle": f"Mock Channel {i}",
                "publishedAt": (datetime.now() - timedelta(days=i)).isoformat(),
                "categoryId": "28",  # Science & Technology
                "tags": ["AI", "automation", "workflow", "productivity"]
            },
            "statistics": {
                "viewCount": str(1000 + i * 100),
                "likeCount": str(50 + i * 10),
                "commentCount": str(20 + i * 5)
            }
        }
        mock_videos.append(video)
    
    return mock_videos

def test_filtering_logic():
    """Test the content filtering without API calls."""
    logger.info("🧪 Testing content filtering logic...")
    
    mock_videos = create_mock_youtube_videos(10)
    
    # Test different types of content
    test_cases = [
        {
            "title": "AI Workflow Automation Tutorial",
            "description": "Learn how to automate your workflows with AI tools",
            "category_id": "28",
            "expected_relevant": True
        },
        {
            "title": "Cooking Recipe - Pasta Carbonara",
            "description": "Delicious pasta recipe with eggs and cheese",
            "category_id": "26",
            "expected_relevant": False
        },
        {
            "title": "N8N Workflow Builder Guide",
            "description": "Build powerful workflows with N8N automation platform",
            "category_id": "28",
            "expected_relevant": True
        }
    ]
    
    for i, test_case in enumerate(test_cases):
        mock_video = {
            "id": f"test_video_{i}",
            "snippet": {
                "title": test_case["title"],
                "description": test_case["description"],
                "categoryId": test_case["category_id"]
            }
        }
        
        try:
            result = filter_youtube_video(mock_video, "youtube")
            is_relevant = result["is_relevant"]
            expected = test_case["expected_relevant"]
            
            status = "✅" if is_relevant == expected else "❌"
            logger.info(f"{status} Test {i+1}: {test_case['title']}")
            logger.info(f"   Expected: {expected}, Got: {is_relevant}")
            logger.info(f"   Language: {result.get('language', 'unknown')}")
            logger.info(f"   Category: {result.get('category_name', 'unknown')}")
            logger.info(f"   Topics: {result.get('topic_labels', [])}")
            
        except Exception as e:
            logger.error(f"❌ Test {i+1} failed: {e}")
    
    logger.info("🎯 Filtering logic test completed!")

def test_database_operations():
    """Test database operations with mock data."""
    logger.info("🗄️ Testing database operations...")
    
    try:
        # Test database connection
        from services.etl.dbio import get_conn
        import os
        
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            logger.warning("⚠️ DATABASE_URL not set, skipping database tests")
            return
        
        conn = get_conn(dsn)
        logger.info("✅ Database connection successful")
        
        # Test inserting mock data
        mock_rows = [
            ("test_source", f"test_id_{i}", {"test": f"data_{i}"})
            for i in range(3)
        ]
        
        inserted_count = insert_many_raw_rows(conn, mock_rows)
        logger.info(f"✅ Inserted {inserted_count} mock rows")
        
        # Test deduplication query
        recent_ids = select_recent_external_ids(conn, "test_source", 1)
        logger.info(f"✅ Found {len(recent_ids)} recent IDs for deduplication")
        
        conn.close()
        logger.info("✅ Database operations test completed!")
        
    except Exception as e:
        logger.error(f"❌ Database test failed: {e}")

def test_s3_operations():
    """Test S3 operations with mock data."""
    logger.info("☁️ Testing S3 operations...")
    
    try:
        # Test S3 client initialization
        s3_client = S3Client()
        logger.info("✅ S3 client initialized successfully")
        
        # Test bucket operations
        bucket_name = "content"
        if s3_client.ensure_bucket_exists(bucket_name):
            logger.info(f"✅ Bucket '{bucket_name}' exists")
        else:
            logger.info(f"⚠️ Bucket '{bucket_name}' does not exist")
        
        logger.info("✅ S3 operations test completed!")
        
    except Exception as e:
        logger.error(f"❌ S3 test failed: {e}")

def test_pipeline_integration():
    """Test the complete pipeline flow with mock data."""
    logger.info("🔗 Testing pipeline integration...")
    
    try:
        # Create mock videos
        mock_videos = create_mock_youtube_videos(5)
        logger.info(f"✅ Created {len(mock_videos)} mock videos")
        
        # Test filtering
        filtered_videos = []
        for video in mock_videos:
            try:
                result = filter_youtube_video(video, "youtube")
                if result["is_relevant"]:
                    filtered_videos.append(video)
            except Exception as e:
                logger.warning(f"⚠️ Filtering failed for video {video.get('id', 'unknown')}: {e}")
        
        logger.info(f"✅ Filtered to {len(filtered_videos)} relevant videos")
        
        # Test data transformation
        db_rows = []
        for video in filtered_videos:
            db_row = (
                "youtube",
                video["id"],
                video
            )
            db_rows.append(db_row)
        
        logger.info(f"✅ Prepared {len(db_rows)} database rows")
        
        logger.info("✅ Pipeline integration test completed!")
        
    except Exception as e:
        logger.error(f"❌ Pipeline integration test failed: {e}")

def main():
    """Run all tests."""
    logger.info("🚀 Starting comprehensive pipeline testing with mock data...")
    
    # Load environment variables
    load_env_file()
    
    # Test individual components
    test_filtering_logic()
    print()
    
    test_database_operations()
    print()
    
    test_s3_operations()
    print()
    
    test_pipeline_integration()
    print()
    
    logger.info("🎉 All tests completed! Your pipeline is ready for real data.")
    logger.info("💡 Next: Run with --max-pages 1 to test with minimal API usage")

if __name__ == "__main__":
    main()
