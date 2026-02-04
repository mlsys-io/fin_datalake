"""
MinIO Service - Storage bucket management for ETL framework.

This service provides utilities for managing MinIO/S3 buckets during
deployment and setup, separate from ETL pipeline logic.
"""
import os
from typing import Optional, List
from dataclasses import dataclass
from loguru import logger


@dataclass
class MinIOConfig:
    """MinIO connection configuration."""
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    ca_cert_path: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> "MinIOConfig":
        """Create config from environment variables."""
        return cls(
            endpoint_url=os.environ.get("AWS_ENDPOINT_URL", ""),
            access_key=os.environ.get("AWS_ACCESS_KEY_ID", ""),
            secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            region=os.environ.get("AWS_REGION", "us-east-1"),
            ca_cert_path=os.environ.get("CA_PATH"),
        )


class MinIOService:
    """
    MinIO/S3 storage management service.
    
    Used for infrastructure setup operations like creating buckets,
    NOT for data operations (which go through Delta Lake).
    
    Example:
        >>> minio = MinIOService.from_env()
        >>> minio.create_bucket("delta-lake")
        >>> minio.list_buckets()
    """
    
    def __init__(self, config: MinIOConfig):
        self.config = config
        self._client = None
        
    @classmethod
    def from_env(cls) -> "MinIOService":
        """Create service from environment variables."""
        return cls(MinIOConfig.from_env())
    
    @property
    def client(self):
        """Lazy-load boto3 S3 client."""
        if self._client is None:
            import boto3
            
            # Determine SSL verification
            verify = True
            if self.config.ca_cert_path and os.path.exists(self.config.ca_cert_path):
                verify = self.config.ca_cert_path
            
            self._client = boto3.client(
                's3',
                endpoint_url=self.config.endpoint_url,
                aws_access_key_id=self.config.access_key,
                aws_secret_access_key=self.config.secret_key,
                region_name=self.config.region,
                verify=verify,
            )
        return self._client
    
    def list_buckets(self) -> List[str]:
        """List all buckets."""
        response = self.client.list_buckets()
        return [b['Name'] for b in response.get('Buckets', [])]
    
    def bucket_exists(self, bucket_name: str) -> bool:
        """Check if a bucket exists."""
        try:
            self.client.head_bucket(Bucket=bucket_name)
            return True
        except self.client.exceptions.ClientError:
            return False
        except Exception:
            return False
    
    def create_bucket(self, bucket_name: str) -> bool:
        """
        Create a bucket if it doesn't exist.
        
        Returns:
            True if bucket was created, False if it already existed.
        """
        if self.bucket_exists(bucket_name):
            logger.info(f"Bucket '{bucket_name}' already exists")
            return False
        
        try:
            self.client.create_bucket(Bucket=bucket_name)
            logger.success(f"✅ Created bucket: {bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create bucket '{bucket_name}': {e}")
            raise
    
    def delete_bucket(self, bucket_name: str, force: bool = False) -> bool:
        """
        Delete a bucket.
        
        Args:
            bucket_name: Name of the bucket to delete.
            force: If True, delete all objects first (DANGEROUS!).
            
        Returns:
            True if bucket was deleted, False if it didn't exist.
        """
        if not self.bucket_exists(bucket_name):
            logger.warning(f"Bucket '{bucket_name}' does not exist")
            return False
        
        if force:
            logger.warning(f"Force deleting all objects in '{bucket_name}'...")
            self._empty_bucket(bucket_name)
        
        try:
            self.client.delete_bucket(Bucket=bucket_name)
            logger.success(f"🗑️ Deleted bucket: {bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete bucket '{bucket_name}': {e}")
            raise
    
    def _empty_bucket(self, bucket_name: str):
        """Delete all objects in a bucket."""
        paginator = self.client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' not in page:
                continue
            
            objects = [{'Key': obj['Key']} for obj in page['Contents']]
            self.client.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects}
            )
            logger.info(f"Deleted {len(objects)} objects from '{bucket_name}'")
    
    def ensure_buckets(self, bucket_names: List[str]) -> dict:
        """
        Ensure multiple buckets exist (create if missing).
        
        Returns:
            Dict of {bucket_name: created (bool)}
        """
        results = {}
        for name in bucket_names:
            results[name] = self.create_bucket(name)
        return results
