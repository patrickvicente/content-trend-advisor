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
from services.etl.youtube_client import (
    get_most_popular, 
    hydrate_videos, 
    list_channel_upload_ids,
    list_channel_upload_ids_by_handle,
    search_video_ids,
    get_quota_manager,
    check_quota_status,
    QuotaExceeded
)

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

def run_keywords_program(keywords: List[str], 
                        relevance_language: List[str],
                        published_after_iso: Optional[str],
                        max_pages: int) -> List[Dict[str, Any]]:
    """
    Run keywords program to search for videos by keyword.
    
    Args:
        keywords: List of search terms
        relevance_language: List of allowed languages
        published_after_iso: ISO date string for filtering
        max_pages: Maximum pages to fetch per keyword
    
    Returns:
        List of video dictionaries
    """
    videos = []
    all_video_ids = []
    seen = set()  # Dedupe across keywords
    
    if not keywords:
        logger.info("No keywords provided")
        return videos
        
    for keyword in keywords:
        try:
            logger.info(f"Searching for keyword: {keyword}")
            video_ids = search_video_ids(
                query=keyword,
                relevance_language=relevance_language[0] if relevance_language else None,
                published_after_iso=published_after_iso,
                max_pages=max_pages
            )
            
            if video_ids:
                # Deduplicate video IDs across keywords
                new_ids = [vid_id for vid_id in video_ids if vid_id not in seen]
                all_video_ids.extend(new_ids)
                seen.update(new_ids)
                logger.info(f"Found {len(new_ids)} new videos for keyword: {keyword}")
            else:
                logger.info(f"No videos found for keyword: {keyword}")
                
        except QuotaExceeded as e:
            logger.error(f"🚫 Quota exceeded while searching for keyword '{keyword}': {e}")
            logger.info("💡 Consider reducing max_pages or waiting for quota reset")
            break  # Stop processing more keywords if quota is exhausted
        except Exception as e:
            logger.error(f"Error searching for keyword '{keyword}': {e}")
            continue
    
    # Hydrate all video IDs at once for efficiency
    if all_video_ids:
        try:
            logger.info(f"Hydrating {len(all_video_ids)} videos from keywords")
            videos = hydrate_videos(all_video_ids)
            logger.info(f"Successfully hydrated {len(videos)} keyword videos")
        except QuotaExceeded as e:
            logger.error(f"🚫 Quota exceeded while hydrating keyword videos: {e}")
            logger.info("💡 Videos found but couldn't hydrate due to quota limits")
        except Exception as e:
            logger.error(f"Error hydrating keyword videos: {e}")
            
    return videos

def run_competitors_program(handles: List[str], max_pages: int) -> List[Dict[str, Any]]:
    """
    Run competitors program to fetch YouTube videos from specific channels by handle.

    Args:
        handles: List of channel handles (e.g., ['@MrBeast', '@GoogleDevelopers'])
        max_pages: Maximum number of pages to fetch per channel

    Returns:
        List of video dictionaries
    """
    videos = []
    all_video_ids = []
    seen = set()  # Dedupe across channels
    
    if not handles:
        logger.info("No channel handles provided")
        return videos
        
    for handle in handles:
        try:
            # Use the new handle-based function instead of channel ID
            video_ids = list_channel_upload_ids_by_handle(handle, max_pages)
            
            if video_ids:
                # Deduplicate video IDs across channels
                new_ids = [vid_id for vid_id in video_ids if vid_id not in seen]
                all_video_ids.extend(new_ids)
                seen.update(new_ids)
                logger.info(f"Found {len(new_ids)} new videos from channel: {handle}")
            else:
                logger.info(f"No videos found for channel: {handle}")
                
        except QuotaExceeded as e:
            logger.error(f"🚫 Quota exceeded while processing {handle}: {e}")
            logger.info("💡 Consider reducing max_pages or waiting for quota reset")
            break  # Stop processing more channels if quota is exhausted
        except Exception as e:
            logger.error(f"Error fetching videos for channel {handle}: {e}")
            continue
    
    # Hydrate all video IDs at once for efficiency
    if all_video_ids:
        try:
            logger.info(f"Hydrating {len(all_video_ids)} videos from competitors")
            videos = hydrate_videos(all_video_ids)
            logger.info(f"Successfully hydrated {len(videos)} competitor videos")
        except QuotaExceeded as e:
            logger.error(f"🚫 Quota exceeded while hydrating videos: {e}")
            logger.info("💡 Videos found but couldn't hydrate due to quota limits")
        except Exception as e:
            logger.error(f"Error hydrating competitor videos: {e}")
            
    return videos

def run_trending_program(regions: List[str], max_pages: int) -> List[Dict[str, Any]]:
    """
    Run trending program to fetch most popular videos by region.
    
    Args:
        regions: List of region codes (e.g., ['US', 'GB', 'CA'])
        max_pages: Maximum pages to fetch per region
    
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
            logger.info(f"Fetching trending videos for region: {region}")
            region_videos = get_most_popular(region, max_pages)
            
            if region_videos:
                # Extract video IDs and deduplicate
                for video in region_videos:
                    video_id = extract_video_id(video)
                    if video_id and video_id not in seen:
                        seen.add(video_id)
                        videos.append(video)
                        all_video_ids.append(video_id)
                
                logger.info(f"Found {len(region_videos)} trending videos for region: {region}")
            else:
                logger.info(f"No trending videos found for region: {region}")
                
        except QuotaExceeded as e:
            logger.error(f"🚫 Quota exceeded while fetching trending for region '{region}': {e}")
            logger.info("💡 Consider reducing max_pages or waiting for quota reset")
            break  # Stop processing more regions if quota is exhausted
        except Exception as e:
            logger.error(f"Error fetching trending videos for region '{region}': {e}")
            continue
    
    logger.info(f"Total unique trending videos: {len(videos)}")
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
                        channel_handles: List[str] = None,
                        regions: List[str] = None,
                        relevance_lang: List[str] = None,
                        max_pages: int = 2,
                        published_after_iso: Optional[str] = None,
                        allowed_categories: List[str] = None,
                        denied_categories: List[str] = None) -> IngestReport:
    """
    Main orchestration function for YouTube ingestion pipeline.
    
    Args:
        programs: List of programs to run ['keywords', 'competitors', 'trending']
        keywords: List of search keywords for keywords program
        channel_handles: List of channel handles for competitors program
        regions: List of region codes for trending program
        relevance_lang: List of allowed languages
        max_pages: Maximum pages to fetch per program
        published_after_iso: ISO date string for filtering videos
        allowed_categories: List of allowed video categories
        denied_categories: List of denied video categories
    
    Returns:
        IngestReport with summary and data
    """
    # Initialize quota manager and log status
    quota_manager = get_quota_manager()
    quota_status = check_quota_status()
    
    logger.info("🔋 YouTube API Quota Status:")
    logger.info(f"   📊 Remaining: {quota_status['remaining']:,} units")
    logger.info(f"   📈 Used: {quota_status['used']:,} units")
    logger.info(f"   🎯 Daily Cap: {quota_status['daily_cap']:,} units")
    logger.info(f"   ⏰ Resets at: {quota_status['will_reset_at']}")
    
    # Check if we have enough quota for basic operations
    if quota_status['remaining'] < 100:
        logger.warning("⚠️  Low quota remaining! Consider reducing max_pages or waiting for reset.")
    
    # Estimate quota usage for this run
    quota_estimate = estimate_quota_usage(
        programs=programs,
        keywords=keywords,
        channel_handles=channel_handles,
        regions=regions,
        max_pages=max_pages
    )
    
    logger.info("📊 Quota Usage Estimate:")
    logger.info(f"   💰 Total estimated: {quota_estimate['total_estimated']:,} units")
    logger.info(f"   📋 Recommendation: {quota_estimate['recommendation']}")
    
    for program, details in quota_estimate['breakdown'].items():
        logger.info(f"   🔹 {program}: {details['cost']:,} units ({details['description']})")
    
    if not quota_estimate['will_fit']:
        logger.error(f"❌ Insufficient quota! Need {quota_estimate['total_estimated']:,} but only {quota_estimate['remaining']:,} remaining.")
        logger.info("💡 Consider reducing max_pages or waiting for quota reset.")
        return report
    
    programs = programs or []
    keywords = keywords or []
    channel_handles = channel_handles or []
    relevance_lang = relevance_lang or ["en"]
    
    logger.info(f"""
        Starting YouTube Ingestion Pipeline:
        🎯 Programs: {programs}
        🔍 Keywords: {len(keywords)} terms
        📺 Channels: {len(channel_handles)} channels  
        🌍 Regions: {regions}
        🗣️ Languages: {relevance_lang}
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
                keywords=keywords,
                relevance_language=relevance_lang,
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
        if "competitors" in programs and channel_handles:
            logger.info("📺 Running competitors program...")
            competitor_videos = run_competitors_program(
                handles=channel_handles,
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
    
    # Final quota status summary
    final_quota_status = check_quota_status()
    logger.info("🔋 Final Quota Status:")
    logger.info(f"   📊 Remaining: {final_quota_status['remaining']:,} units")
    logger.info(f"   📈 Used this run: {quota_status['remaining'] - final_quota_status['remaining']:,} units")
    logger.info(f"   🎯 Daily Cap: {final_quota_status['daily_cap']:,} units")
    
    if final_quota_status['remaining'] < 100:
        logger.warning("⚠️  Low quota remaining! Consider reducing max_pages for future runs.")
    
    return report

def estimate_quota_usage(programs: List[str],
                         keywords: List[str] = None,
                         channel_handles: List[str] = None,
                         regions: List[str] = None,
                         max_pages: int = 2) -> Dict[str, Any]:
    """
    Estimate quota usage for the planned pipeline run.
    
    Returns:
        Dict with estimated costs and recommendations
    """
    quota_manager = get_quota_manager()
    total_estimated = 0
    breakdown = {}
    
    if 'keywords' in programs and keywords:
        # search.list costs 100 units per page
        cost = len(keywords) * max_pages * 100
        breakdown['keywords'] = {
            'cost': cost,
            'description': f"{len(keywords)} keywords × {max_pages} pages × 100 units"
        }
        total_estimated += cost
    
    if 'competitors' in programs and channel_handles:
        # channels.list (1) + playlistItems.list (1) per page per channel
        cost = len(channel_handles) * (1 + max_pages)
        breakdown['competitors'] = {
            'cost': cost,
            'description': f"{len(channel_handles)} channels × (1 + {max_pages} pages) units"
        }
        total_estimated += cost
    
    if 'trending' in programs and regions:
        # videos.list costs 1 unit per page per region
        cost = len(regions) * max_pages
        breakdown['trending'] = {
            'cost': cost,
            'description': f"{len(regions)} regions × {max_pages} pages × 1 unit"
        }
        total_estimated += cost
    
    # Hydration costs (1 unit per 50 videos)
    estimated_videos = 0
    if 'keywords' in programs and keywords:
        estimated_videos += len(keywords) * max_pages * 50  # Rough estimate
    if 'competitors' in programs and channel_handles:
        estimated_videos += len(channel_handles) * max_pages * 50
    if 'trending' in programs and regions:
        estimated_videos += len(regions) * max_pages * 50
    
    hydration_cost = max(1, estimated_videos // 50)  # 1 unit per 50 videos
    breakdown['hydration'] = {
        'cost': hydration_cost,
        'description': f"~{estimated_videos} videos ÷ 50 = {hydration_cost} units"
    }
    total_estimated += hydration_cost
    
    return {
        'total_estimated': total_estimated,
        'breakdown': breakdown,
        'will_fit': quota_manager.remaining() >= total_estimated,
        'remaining': quota_manager.remaining(),
        'recommendation': '✅ Safe to run' if quota_manager.remaining() >= total_estimated else '⚠️ Consider reducing max_pages'
    }

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
    parser.add_argument("--channels-file", 
                       default='services/etl/config/channels_seed.csv',
                       help='Path to CSV file containing channel handles')
    
    parser.add_argument('--check-quota', 
                       action='store_true',
                       help='Check YouTube API quota status and exit')
    
    parser.add_argument('--estimate-quota', 
                       action='store_true',
                       help='Estimate quota usage for planned run and exit')
    
    parser.add_argument("--regions", default="US,IN,PH, AU", 
                       help="Comma-separated region codes")
    parser.add_argument("--relevance-lang", default="en", 
                       help="Comma-separated language codes")
    parser.add_argument("--max-pages", 
                       type=int, 
                       default=2,
                       help='Maximum pages to fetch per program/query (default: 2)')
    parser.add_argument("--published-after", 
                       help="ISO timestamp for recent content filter")
    
    args = parser.parse_args()
    
    # Check quota if requested
    if args.check_quota:
        try:
            quota_status = check_quota_status()
            print("\n🔋 YouTube API Quota Status")
            print("=" * 50)
            print(f"📊 Remaining: {quota_status['remaining']:,} units")
            print(f"📈 Used: {quota_status['used']:,} units")
            print(f"🎯 Daily Cap: {quota_status['daily_cap']:,} units")
            print(f"⏰ Resets at: {quota_status['will_reset_at']}")
            
            if quota_status['remaining'] < 100:
                print("\n⚠️  Low quota remaining! Consider reducing max_pages or waiting for reset.")
            elif quota_status['remaining'] < 1000:
                print("\n🟡 Moderate quota remaining. Monitor usage carefully.")
            else:
                print("\n✅ Plenty of quota remaining for normal operations.")
                
            return
        except Exception as e:
            print(f"❌ Error checking quota: {e}")
            return
    
    # Estimate quota if requested
    if args.estimate_quota:
        try:
            # Parse the same arguments that would be used for the actual run
            programs = [p.strip() for p in args.programs.split(',')] if args.programs else []
            keywords = [k.strip() for k in args.keywords.split(',')] if args.keywords else []
            channel_handles = []
            if args.channels_file and os.path.exists(args.channels_file):
                with open(args.channels_file, 'r') as f:
                    reader = csv.DictReader(f)
                    channel_handles = [row['channel_handle'] for row in reader if row.get('channel_handle')]
            regions = [r.strip() for r in args.regions.split(',')] if args.regions else []
            
            quota_estimate = estimate_quota_usage(
                programs=programs,
                keywords=keywords,
                channel_handles=channel_handles,
                regions=regions,
                max_pages=args.max_pages
            )
            
            print("\n📊 YouTube API Quota Usage Estimate")
            print("=" * 50)
            print(f"💰 Total estimated: {quota_estimate['total_estimated']:,} units")
            print(f"📋 Recommendation: {quota_estimate['recommendation']}")
            print(f"🔋 Remaining quota: {quota_estimate['remaining']:,} units")
            print("\n📋 Breakdown by program:")
            
            for program, details in quota_estimate['breakdown'].items():
                print(f"   🔹 {program}: {details['cost']:,} units")
                print(f"      {details['description']}")
            
            if quota_estimate['will_fit']:
                print(f"\n✅ Safe to run! You'll have {quota_estimate['remaining'] - quota_estimate['total_estimated']:,} units remaining.")
            else:
                print(f"\n⚠️  Insufficient quota! Need {quota_estimate['total_estimated']:,} but only {quota_estimate['remaining']:,} remaining.")
                print("💡 Consider reducing max_pages or waiting for quota reset.")
                
            return
        except Exception as e:
            print(f"❌ Error estimating quota: {e}")
            return
    
    # Set up logging
    logging.basicConfig(level=logging.INFO)
    
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
    
    # Load channel handles from CSV (updated for handle-based approach)
    channel_handles = []
    if "competitors" in programs:
        try:
            import csv
            with open(args.channels_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    channel_handle = row.get('channel_handle', '').strip()
                    if channel_handle:
                        channel_handles.append(channel_handle)
                        logger.debug(f"Loaded channel: {channel_handle} ({row.get('channel_name', 'Unknown')})")
            logger.info(f"Loaded {len(channel_handles)} channel handles from {args.channels_file}")
        except FileNotFoundError:
            logger.error(f"Channels file not found: {args.channels_file}")
            channel_handles = []
        except Exception as e:
            logger.error(f"Error loading channels file: {e}")
            channel_handles = []
    
    # Parse other arguments
    regions = [r.strip() for r in args.regions.split(",")]
    relevance_lang = [l.strip() for l in args.relevance_lang.split(",")]
    
    try:
        # Run the pipeline
        report = run_ingest_pipeline(
            programs=programs,
            keywords=keywords,
            channel_handles=channel_handles,
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