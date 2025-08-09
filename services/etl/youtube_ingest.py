import logging
import os
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timezone
from pathlib import Path

# Configure logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
def load_env_file():
    """Load environment variables from .env file if it exists."""
    # Try to find .env file in project root (go up from services/etl/)
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent  # Go up 2 levels: etl -> services -> project_root
    env_file = project_root / '.env'
    
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
            logger.info(f"✅ Loaded environment variables from {env_file}")
        except Exception as e:
            logger.warning(f"⚠️ Could not load .env file: {e}")
    else:
        logger.info(f"ℹ️ No .env file found at {env_file}")

# Load .env file when module is imported
load_env_file()

from services.etl.dbio import get_conn, insert_many_raw_rows, select_recent_external_ids
from services.etl.filters import filter_content
from services.etl.s3io import get_default_s3_client
from services.etl.youtube_client import get_most_popular, hydrate_videos, list_channel_upload_ids, search_video_ids

# Global config cache
_config_cache = {}


class IngestReport:
    """Report for YouTube ingestion pipeline results."""
    
    def __init__(self):
        self.total_fetched: int = 0
        self.total_after_filters: int = 0
        self.total_inserted: int = 0
        self.s3_partition: str = ""
        self.s3_prefix_raw: str = "raw/youtube/"
        self.sample_keys: List[str] = []
        self.program_breakdown: Dict[str, int] = {}
        self.start_time: datetime = datetime.now(timezone.utc)
        self.end_time: Optional[datetime] = None
    
    def finalize(self):
        """Mark the report as complete."""
        self.end_time = datetime.now(timezone.utc)
        duration = self.end_time - self.start_time
        logger.info(f"Ingestion completed in {duration.total_seconds():.2f} seconds")
    
    def summary(self) -> str:
        """Return a formatted summary of the ingestion."""
        return f"""
YouTube Ingestion Report:
  📊 Total Fetched: {self.total_fetched}
  ✅ After Filters: {self.total_after_filters}
  💾 Inserted: {self.total_inserted}
  📁 S3 Partition: {self.s3_partition}
  🔗 Sample Keys: {len(self.sample_keys)}
  📈 Program Breakdown: {self.program_breakdown}
  ⏱️  Duration: {(self.end_time - self.start_time).total_seconds():.2f}s
        """.strip()

def run_keywords_program(queries: List[str],
                        relevance_lang: List[str],
                        published_after_iso: Optional[str],
                        max_pages: int) -> List[Dict[str, Any]]:
    """
    Run keywords program to fetch YouTube videos using search queries.

    Args:
        queries: List of search queries
        relevance_lang: List of allowed languages for search
        published_after_iso: ISO timestamp for filtering recent content
        max_pages: Maximum number of pages to fetch per query/lang combo

    Returns:
        List of video dictionaries
    """
    video_ids = []
    seen = set()  # Dedupe across search queries
    
    for query in queries:
        for lang in relevance_lang:
            try:
                search_ids = search_video_ids(query, lang, published_after_iso, max_pages)
                if search_ids:
                    # Deduplicate video IDs
                    new_ids = [vid_id for vid_id in search_ids if vid_id not in seen]
                    video_ids.extend(new_ids)
                    seen.update(new_ids)
                    logger.info(f"Found {len(new_ids)} new videos for query '{query}' in lang '{lang}'")
                else:
                    logger.info(f"No videos found for query: {query} in lang: {lang}")
            
            except Exception as e:
                logger.error(f"Error fetching videos for query '{query}' in lang '{lang}': {e}")
                continue
    
    # Hydrate video IDs to get full video data
    if video_ids:
        try:
            logger.info(f"Hydrating {len(video_ids)} unique video IDs")
            videos = hydrate_videos(video_ids)
            logger.info(f"Successfully hydrated {len(videos)} videos")
            return videos
        except Exception as e:
            logger.error(f"Error hydrating videos: {e}")
            return []
    else:
        logger.info("No video IDs to hydrate")
        return []

def run_competitors_program(channel_ids: List[str], max_pages: int) -> List[Dict[str, Any]]:
    """
    Run competitors program to fetch YouTube videos from specific channels.

    Args:
        channel_ids: List of channel IDs to monitor
        max_pages: Maximum number of pages to fetch per channel

    Returns:
        List of video dictionaries
    """
    videos = []
    all_video_ids = []
    seen = set()  # Dedupe across channels
    
    if not channel_ids:
        logger.info("No channel IDs provided")
        return videos
        
    for channel_id in channel_ids:
        try:
            video_ids = list_channel_upload_ids(channel_id, max_pages)
            
            if video_ids:
                # Deduplicate video IDs across channels
                new_ids = [vid_id for vid_id in video_ids if vid_id not in seen]
                all_video_ids.extend(new_ids)
                seen.update(new_ids)
                logger.info(f"Found {len(new_ids)} new videos from channel: {channel_id}")
            else:
                logger.info(f"No videos found for channel: {channel_id}")
                
        except Exception as e:
            logger.error(f"Error fetching videos for channel {channel_id}: {e}")
            continue
    
    # Hydrate all video IDs at once for efficiency
    if all_video_ids:
        try:
            logger.info(f"Hydrating {len(all_video_ids)} videos from competitors")
            videos = hydrate_videos(all_video_ids)
            logger.info(f"Successfully hydrated {len(videos)} competitor videos")
        except Exception as e:
            logger.error(f"Error hydrating competitor videos: {e}")
            
    return videos

def run_trending_program(regions: List[str], max_pages: int) -> List[Dict[str, Any]]:
    """
    Run trending program to fetch YouTube videos from trending feeds.

    Args:
        regions: List of region codes (e.g., ['US', 'IN', 'PH'])
        max_pages: Maximum number of pages to fetch per region

    Returns:
        List of video dictionaries
    """
    videos = []
    all_video_ids = []
    seen = set()  # Dedupe across regions
    
    if not regions:
        logger.info("No regions provided")
        return videos
        
    for region in regions:
        try:
            trending_ids = get_most_popular(region, max_pages)
            if trending_ids:
                # Deduplicate trending IDs across regions  
                new_ids = [vid_id for vid_id in trending_ids if vid_id not in seen]
                all_video_ids.extend(new_ids)
                seen.update(new_ids)
                logger.info(f"Found {len(new_ids)} new trending videos from region: {region}")
            else:
                logger.info(f"No trending videos found for region: {region}")
        except Exception as e:
            logger.error(f"Error fetching trending videos for region {region}: {e}")
            continue
    
    # Hydrate all trending video IDs
    if all_video_ids:
        try:
            logger.info(f"Hydrating {len(all_video_ids)} trending videos")
            videos = hydrate_videos(all_video_ids)
            logger.info(f"Successfully hydrated {len(videos)} trending videos")
        except Exception as e:
            logger.error(f"Error hydrating trending videos: {e}")
            
    return videos

def apply_relevance_filters(videos: List[Dict[str, Any]],
                            allowed_languages: List[str] = None,
                            allowed_categories: List[str] = None,
                            denied_categories: List[str] = None) -> List[Dict[str, Any]]:
    """
    Apply comprehensive relevance filters to videos using the filters.py module.
    
    Args:
        videos: List of YouTube video dictionaries
        allowed_languages: List of allowed language codes (defaults to ['en'])
        allowed_categories: List of allowed category names
        denied_categories: List of denied category names
        
    Returns:
        List of filtered video dictionaries with filter metadata added
    """
    if not videos:
        return []
        
    filtered_videos = []
    filter_stats = {
        'total': len(videos),
        'language_filtered': 0,
        'category_filtered': 0,
        'topic_filtered': 0,
        'passed': 0
    }
    
    for video in videos:
        try:
            # Extract video metadata
            snippet = video.get('snippet', {})
            title = snippet.get('title', '')
            description = snippet.get('description', '')
            category_id = snippet.get('categoryId')
            
            # Skip videos without essential metadata
            if not title:
                logger.debug(f"Skipping video {video.get('id', 'unknown')} - no title")
                continue
            
            # Apply comprehensive filtering using filters.py
            filter_result = filter_content(
                title=title,
                description=description,
                category_id=category_id,
                allowed_languages=allowed_languages,
                allowed_categories=allowed_categories,
                denied_categories=denied_categories
            )
            
            # Track filter statistics
            if not filter_result['language_ok']:
                filter_stats['language_filtered'] += 1
            if not filter_result['category_ok']:
                filter_stats['category_filtered'] += 1
            if not filter_result['topics_ok']:
                filter_stats['topic_filtered'] += 1
                
            # Only keep videos that pass all filters
            if filter_result['is_allowed']:
                # Add filter metadata to the video for downstream processing
                video['_filter_metadata'] = {
                    'detected_language': filter_result['language'],
                    'category_name': filter_result['category'],
                    'detected_topics': filter_result['topics'],
                    'filtered_at': datetime.now(timezone.utc).isoformat()
                }
                filtered_videos.append(video)
                filter_stats['passed'] += 1
                
        except Exception as e:
            logger.error(f"Error filtering video {video.get('id', 'unknown')}: {e}")
            continue
    
    # Log comprehensive filter statistics
    logger.info(f"""
Filter Results:
  📊 Total videos: {filter_stats['total']}
  🌐 Language filtered: {filter_stats['language_filtered']}
  📂 Category filtered: {filter_stats['category_filtered']}
  🏷️  Topic filtered: {filter_stats['topic_filtered']}
  ✅ Passed filters: {filter_stats['passed']} ({filter_stats['passed']/filter_stats['total']*100:.1f}%)
    """.strip())
    
    return filtered_videos

def persist_raw_batch(videos: List[Dict[str, Any]], 
                     skip_recent_days: int = 7) -> Tuple[int, List[str]]:
    """
    Persist raw batch of videos to S3 and PostgreSQL with deduplication.
    
    Args:
        videos: List of video dictionaries to persist
        skip_recent_days: Skip videos fetched within this many days
        
    Returns:
        Tuple of (inserted_count, sample_s3_keys)
    """
    if not videos:
        logger.info("No videos to persist")
        return 0, []
    
    sample_keys = []
    conn = None
    
    try:
        # Initialize clients
        s3_client = get_default_s3_client()
        bucket = os.getenv("S3_BUCKET", "content")
        s3_client.ensure_bucket_exists(bucket)
        
        # Get database connection
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            raise ValueError("DATABASE_URL environment variable not set")
        conn = get_conn(dsn)
        
        # Get recently seen video IDs for deduplication
        recent_ids = select_recent_external_ids(conn, "youtube", skip_recent_days)
        logger.info(f"Found {len(recent_ids)} videos fetched in last {skip_recent_days} days")
        
        # Filter out recently seen videos
        new_videos = [v for v in videos if v.get('id') not in recent_ids]
        if len(new_videos) < len(videos):
            logger.info(f"Filtered out {len(videos) - len(new_videos)} recently seen videos")
        
        if not new_videos:
            logger.info("All videos were recently seen, nothing to persist")
            return 0, []
        
        # Prepare batch data for database
        db_rows = []
        s3_operations = []
        
        for video in new_videos:
            video_id = video.get('id')
            if not video_id:
                logger.warning("Video missing ID, skipping")
                continue
                
            # Prepare database row
            db_rows.append(("youtube", video_id, video))
            
            # Prepare S3 operation
            s3_operations.append(video)
        
        # Batch insert to database (more efficient than individual inserts)
        if db_rows:
            inserted_count = insert_many_raw_rows(conn, db_rows)
            logger.info(f"Inserted {inserted_count} new videos to database")
        else:
            inserted_count = 0
        
        # Store raw data to S3 (for analytics/ML pipeline)
        for video in s3_operations[:inserted_count]:  # Only store successfully inserted videos
            try:
                s3_key = s3_client.put_raw_json(bucket, "youtube", video)
                sample_keys.append(s3_key)
            except Exception as e:
                logger.error(f"Failed to store video {video.get('id')} to S3: {e}")
                continue
        
        logger.info(f"""
Persistence Summary:
  📊 Total videos: {len(videos)}
  🔄 Filtered (recent): {len(videos) - len(new_videos)}
  💾 Database inserted: {inserted_count}
  📁 S3 stored: {len(sample_keys)}
        """.strip())
        
        return inserted_count, sample_keys
        
    except Exception as e:
        logger.error(f"Error in persist_raw_batch: {e}")
        raise
    finally:
        if conn:
            conn.close()

    
def run_ingest_pipeline(programs: List[str],
                        keywords: List[str] = None,
                        channel_ids: List[str] = None,
                        regions: List[str] = None,
                        relevance_lang: List[str] = None,
                        max_pages: int = 2,
                        published_after_iso: Optional[str] = None,
                        allowed_categories: List[str] = None,
                        denied_categories: List[str] = None) -> IngestReport:
    """
    Main orchestration function for YouTube ingestion pipeline.
    
    Orchestrates:
      - Run enabled programs (keywords, competitors, trending)
      - Aggregate and deduplicate results by video_id
      - Apply relevance filters
      - Persist to S3 and PostgreSQL
      - Return comprehensive IngestReport
      
    Args:
        programs: List of programs to run ['keywords', 'competitors', 'trending']
        keywords: List of search keywords for keywords program
        channel_ids: List of channel IDs for competitors program
        regions: List of region codes for trending program
        relevance_lang: List of allowed languages
        max_pages: Maximum pages to fetch per program/query
        published_after_iso: ISO timestamp to filter recent content
        allowed_categories: List of allowed YouTube categories
        denied_categories: List of denied YouTube categories
        
    Returns:
        IngestReport with comprehensive statistics
    """
    report = IngestReport()
    report.s3_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Set defaults
    programs = programs or []
    keywords = keywords or []
    channel_ids = channel_ids or []
    regions = regions or []
    relevance_lang = relevance_lang or ["en"]
    
    logger.info(f"""
Starting YouTube Ingestion Pipeline:
  🎯 Programs: {programs}
  🔍 Keywords: {len(keywords)} terms
  📺 Channels: {len(channel_ids)} channels  
  🌍 Regions: {regions}
  🗣️  Languages: {relevance_lang}
  📄 Max Pages: {max_pages}
    """.strip())
    
    # Step 1: Aggregate videos from all enabled programs
    all_videos = []
    seen_video_ids = set()
    
    try:
        # Run Keywords Program
        if "keywords" in programs and keywords:
            logger.info("🔍 Running keywords program...")
            keyword_videos = run_keywords_program(
                queries=keywords,
                relevance_lang=relevance_lang,
                published_after_iso=published_after_iso,
                max_pages=max_pages
            )
            
            # Deduplicate across programs
            new_videos = [v for v in keyword_videos if v.get('id') not in seen_video_ids]
            all_videos.extend(new_videos)
            seen_video_ids.update(v.get('id') for v in new_videos)
            
            report.program_breakdown['keywords'] = len(new_videos)
            logger.info(f"Keywords program: {len(new_videos)} unique videos")
        
        # Run Competitors Program  
        if "competitors" in programs and channel_ids:
            logger.info("📺 Running competitors program...")
            competitor_videos = run_competitors_program(
                channel_ids=channel_ids,
                max_pages=max_pages
            )
            
            new_videos = [v for v in competitor_videos if v.get('id') not in seen_video_ids]
            all_videos.extend(new_videos)
            seen_video_ids.update(v.get('id') for v in new_videos)
            
            report.program_breakdown['competitors'] = len(new_videos)
            logger.info(f"Competitors program: {len(new_videos)} unique videos")
        
        # Run Trending Program
        if "trending" in programs and regions:
            logger.info("🔥 Running trending program...")
            trending_videos = run_trending_program(
                regions=regions,
                max_pages=max_pages
            )
            
            new_videos = [v for v in trending_videos if v.get('id') not in seen_video_ids]
            all_videos.extend(new_videos)
            seen_video_ids.update(v.get('id') for v in new_videos)
            
            report.program_breakdown['trending'] = len(new_videos)
            logger.info(f"Trending program: {len(new_videos)} unique videos")
        
        report.total_fetched = len(all_videos)
        logger.info(f"📊 Total unique videos aggregated: {report.total_fetched}")
        
        # Step 2: Apply relevance filters
        if all_videos:
            logger.info("🔍 Applying relevance filters...")
            filtered_videos = apply_relevance_filters(
                videos=all_videos,
                allowed_languages=relevance_lang,
                allowed_categories=allowed_categories,
                denied_categories=denied_categories
            )
            report.total_after_filters = len(filtered_videos)
        else:
            filtered_videos = []
            report.total_after_filters = 0
        
        # Step 3: Persist to S3 and Database
        if filtered_videos:
            logger.info("💾 Persisting filtered videos...")
            inserted_count, sample_keys = persist_raw_batch(filtered_videos)
            report.total_inserted = inserted_count
            report.sample_keys = sample_keys[:5]  # Keep first 5 as samples
        else:
            logger.info("No videos passed filters, nothing to persist")
            report.total_inserted = 0
            report.sample_keys = []
        
    except Exception as e:
        logger.error(f"Error in ingestion pipeline: {e}")
        raise
    finally:
        report.finalize()
    
    return report

def main():
    """
    CLI entry point for YouTube ingestion pipeline.
    
    Example usage:
      python youtube_ingest.py --programs keywords,trending --max-pages 3
    """
    import argparse
    import yaml
    
    parser = argparse.ArgumentParser(description="YouTube Content Ingestion Pipeline")
    parser.add_argument("--programs", default="keywords", 
                       help="Comma-separated programs: keywords,competitors,trending")
    parser.add_argument("--keywords-file", default="services/etl/config/topics.yml",
                       help="YAML file with search keywords")
    parser.add_argument("--channels-file", default="services/etl/config/channels_seed.csv", 
                       help="CSV file with channel IDs")
    parser.add_argument("--regions", default="US,IN,PH", 
                       help="Comma-separated region codes")
    parser.add_argument("--relevance-lang", default="en", 
                       help="Comma-separated language codes")
    parser.add_argument("--max-pages", type=int, default=2,
                       help="Maximum pages to fetch per program")
    parser.add_argument("--published-after", 
                       help="ISO timestamp for recent content filter")
    
    args = parser.parse_args()
    
    # Parse program list
    programs = [p.strip() for p in args.programs.split(",")]
    
    # Load keywords from YAML config
    keywords = []
    if "keywords" in programs:
        try:
            with open(args.keywords_file, 'r') as f:
                config = yaml.safe_load(f)
                # Extract keywords from topics config
                keywords = config.get("niche_topics", [])
                # keywords.extend(config.get("secondary_topics", []))
            logger.info(f"Loaded {len(keywords)} keywords from config")
        except Exception as e:
            logger.error(f"Error loading keywords file: {e}")
            keywords = ["AI", "automation", "programming"]  # Fallback
    
    # Load channel IDs from CSV
    channel_ids = []
    if "competitors" in programs:
        try:
            import csv
            with open(args.channels_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    channel_id = row.get('channel_id', '').strip()
                    if channel_id:
                        channel_ids.append(channel_id)
                        logger.debug(f"Loaded channel: {channel_id} ({row.get('channel_name', 'Unknown')})")
            logger.info(f"Loaded {len(channel_ids)} channel IDs from {args.channels_file}")
        except FileNotFoundError:
            logger.error(f"Channels file not found: {args.channels_file}")
            channel_ids = []
        except Exception as e:
            logger.error(f"Error loading channels file: {e}")
            channel_ids = []
    
    # Parse other arguments
    regions = [r.strip() for r in args.regions.split(",")]
    relevance_lang = [l.strip() for l in args.relevance_lang.split(",")]
    
    try:
        # Run the pipeline
        report = run_ingest_pipeline(
            programs=programs,
            keywords=keywords,
            channel_ids=channel_ids,
            regions=regions,
            relevance_lang=relevance_lang,
            max_pages=args.max_pages,
            published_after_iso=args.published_after
        )
        
        # Print summary
        print(report.summary())
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()