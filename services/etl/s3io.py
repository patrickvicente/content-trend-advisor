"""
S3/MinIO operations module for Content Trend Advisor ETL pipeline.

This module provides a clean interface for:
- Storing raw JSON data from APIs
- Organizing data with partitioned paths (dt=YYYY-MM-DD pattern)
- Robust error handling and logging
- Future: Reading data for analytics/ML pipelines
"""
import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class S3Client:
    """
    S3/MinIO client for Content Trend Advisor ETL operations.

    Handles raw data storage with proper partitioning and error handling.
    """

    def __init__(self,
                endpoint_url: Optional[str] = None,
                aws_access_key_id: Optional[str] = None,
                aws_secret_access_key: Optional[str] = None,
                region_name: str = "us-east-1"):
        """
        Initialize S3 client with environment variables or explicit params.

        Args:
            endpoint_url: S3 endpoint (for MinIO: http://localhost:9000)
            aws_access_key_id: Access key
            aws_secret_access_key: Secret key  
            region_name: AWS region
        """

        self.endpoint_url = endpoint_url or os.getenv("S3_ENDPOINT")
        self.access_key = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region = region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1")

        if not all([self.endpoint_url, self.access_key, self.secret_key]):
            raise ValueError("Missing required S3 configuration. Check environment variables.")

        # Initialize boto3 client
        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region
        )

        logger.info(f"S3 client initialized for endpoint: {self.endpoint_url}")
    
    def put_raw_json(self, 
                    bucket: str, 
                    source: str, 
                    payload: Dict[Any, Any]) -> str:
        """
        Store raw JSON data with date partitioning.
        
        Args:
            bucket: S3 bucket name (e.g., 'content')
            source: Source identifier (e.g., 'youtube')
            payload: JSON payload to store
        
        Returns:
            str: S3 key where data was stored
        
        Example:
            key = s3.put_raw_json('content', 'youtube', video_data)
            # Stores at: content/raw/youtube/dt=2024-08-09/page_1691234567.89.json
        """
        try:
            # create date-partitioned path (Hive-style partitioning)
            dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            timestamp = datetime.now(timezone.utc).timestamp()

            # build S3 key with proper partitioning
            key = f"raw/{source}/dt={dt}/page_{timestamp}.json"

            # upload JSON data
            self.client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(payload, indent=2),
                ContentType='application/json',
                Metadata={
                    'source': source,
                    'ingested_at': datetime.now(timezone.utc).isoformat(),
                    'content_type': 'raw_json'
                }
            )
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise
        except json.JSONEncodeError as e:
            logger.error(f"Invalid JSON payload: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in put_raw_json: {e}")
            raise

        logger.info(f"Successfully stored data at s3://{bucket}/{key}")
        return key
    
    def get_object(self, bucket: str, key: str) -> bytes:
        """
        Retrieve object from S3
        Args:
            bucket: S3 bucket name
            key: S3 object key
        
        Returns:
            bytes: Object content
        """
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        except ClientError as e:
            logger.error(f"Failed to retrieve s3://{bucket}/{key}: {e}")
            raise
    
    def list_objects(self,
                    bucket:str, 
                    prefix: str="",
                    max_keys: int=1000) -> List[Dict[str, Any]]:
        """
        List objects in S3 bucket with optional prefix and max keys.

        Args:
            bucket: S3 bucket name
            prefix: Key prefix to filter by
            max_keys: Maximum number of objects to return
            
        Returns:
            List[Dict]: List of object metadata
        """
        try:
            response = self.client.list_objects_v2(
                Bucket=bucket, 
                Prefix=prefix, 
                MaxKeys=max_keys
            )
            return response.get("Contents", [])
        except ClientError as e:
            logger.error(f"Failed to list objects in s3://{bucket}/{prefix}: {e}")
            raise
    
    def ensure_bucket_exists(self, bucket: str) -> bool:
        """
        Ensure S3 bucket exists, create if it doesn't.

        Args:
            bucket: S3 bucket name
        Returns:
        bool: True if bucket exists or was created successfully
        """

        try:
            self.client.head_bucket(Bucket=bucket)
            logger.info(f"Bucket '{bucket}' already exists")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    self.client.create_bucket(Bucket=bucket)
                    logger.info(f"Created bucket '{bucket}'")
                    return True
                except ClientError as create_error:
                    logger.error(f"Failed to create bucket '{bucket}': {create_error}")
                    return False
            else:
                logger.error(f"Error checking bucket '{bucket}': {e}")
                return False

    def put_features_parquet(self, bucket: str, source: str, dt: str, local_path: str) -> str:
        """
        Upload a local parquet file to: features/{source}/dt={dt}/features.parquet
        
        Args:
            bucket: S3 bucket name
            source: Data source identifier (e.g., 'youtube', 'reddit')
            dt: Date partition in YYYY-MM-DD format
            local_path: Path to local parquet file
            
        Returns:
            str: S3 key where parquet file was stored
            
        Example:
            key = s3.put_features_parquet('content', 'youtube', '2024-08-09', '/tmp/features.parquet')
            # Stores at: content/features/youtube/dt=2024-08-09/features.parquet
        """
        try:
            # Build S3 key with Hive-style partitioning for features
            key = f"features/{source}/dt={dt}/features.parquet"
            
            # Upload parquet file
            with open(local_path, 'rb') as file_data:
                self.client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=file_data,
                    ContentType='application/octet-stream',
                    Metadata={
                        'source': source,
                        'partition_date': dt,
                        'uploaded_at': datetime.now(timezone.utc).isoformat(),
                        'content_type': 'parquet',
                        'data_type': 'features'
                    }
                )
                
            logger.info(f"Successfully uploaded parquet file to s3://{bucket}/{key}")
            return key
            
        except FileNotFoundError:
            logger.error(f"Local parquet file not found: {local_path}")
            raise
        except ClientError as e:
            logger.error(f"Failed to upload parquet to S3: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in put_features_parquet: {e}")
            raise

    def list_prefix(self, bucket: str, prefix: str, recursive: bool = True) -> List[str]:
        """
        List keys under a prefix for debugging/validation.
        
        Args:
            bucket: S3 bucket name
            prefix: Key prefix to search under
            recursive: If True, lists all keys recursively under prefix
            
        Returns:
            List[str]: List of object keys matching the prefix
            
        Example:
            keys = s3.list_prefix('content', 'features/youtube/')
            # Returns: ['features/youtube/dt=2024-08-09/features.parquet', ...]
        """
        try:
            keys = []
            
            # Use paginator to handle large result sets
            paginator = self.client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        
                        # If not recursive, only include direct children (no additional slashes after prefix)
                        if not recursive:
                            remaining_path = key[len(prefix):]
                            if '/' in remaining_path.rstrip('/'):
                                continue
                                
                        keys.append(key)
            
            logger.info(f"Found {len(keys)} objects under prefix: s3://{bucket}/{prefix}")
            return keys
            
        except ClientError as e:
            logger.error(f"Failed to list objects under prefix s3://{bucket}/{prefix}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in list_prefix: {e}")
            raise

# Convenience functions for common operations
def get_default_s3_client() -> S3Client:
    """Get S3 client with default environment configuration."""
    return S3Client()

def store_raw_data(source: str, payload: Dict[Any, Any], bucket: str = None) -> str:
    """
    Convenience function to store raw data with default configuration.
    
    Args:
        source: Data source (youtube, reddit, etc.)
        payload: JSON data to store
        bucket: S3 bucket (defaults to S3_BUCKET env var)
        
    Returns:
        str: S3 key where data was stored
    """
    bucket = bucket or os.getenv("S3_BUCKET")
    if not bucket:
        raise ValueError("No bucket specified and S3_BUCKET environment variable not set")
    
    s3_client = get_default_s3_client()
    s3_client.ensure_bucket_exists(bucket)
    return s3_client.put_raw_json(bucket, source, payload)

# Example usage and testing
if __name__ == "__main__":
    # Test the S3 client
    try:
        # Initialize client
        s3 = get_default_s3_client()
        
        # Test data
        test_payload = {
            "id": "test123",
            "snippet": {"title": "Test Video", "description": "Test description"},
            "statistics": {"viewCount": "1000", "likeCount": "50"}
        }
        
        # Store test data
        bucket = os.getenv("S3_BUCKET", "content")
        s3.ensure_bucket_exists(bucket)
        key = s3.put_raw_json(bucket, "youtube", test_payload)
        print(f"✅ Test data stored at: {key}")
        
        # List objects to verify
        objects = s3.list_objects(bucket, prefix="raw/youtube/")
        print(f"📋 Found {len(objects)} objects in raw/youtube/")
        
        # Retrieve and verify
        data = s3.get_object(bucket, key)
        retrieved = json.loads(data.decode('utf-8'))
        print(f"✅ Retrieved data: {retrieved['snippet']['title']}")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise
