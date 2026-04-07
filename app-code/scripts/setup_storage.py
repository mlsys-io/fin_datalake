#!/usr/bin/env python3
"""
Setup Storage - Initialize MinIO buckets for ETL framework.

This script creates the required buckets in MinIO during deployment.
Run this after configuring your environment with setup-config.sh.

Usage:
    python -m scripts.setup_storage
    
    # Or with custom buckets:
    python -m scripts.setup_storage --buckets delta-lake raw-data
"""
import argparse
import sys
from typing import List

from loguru import logger


# Default buckets required by the ETL framework
DEFAULT_BUCKETS = [
    "delta-lake",      # Primary Delta Lake storage
    "raw-data",        # Raw data ingestion
    "checkpoints",     # Streaming checkpoints
]


def setup_storage(buckets: List[str]) -> bool:
    """
    Create required MinIO buckets.
    
    Returns:
        True if all buckets are ready, False on any failure.
    """
    from etl.services.minio import MinIOService
    
    logger.info("🪣 Setting up MinIO storage buckets...")
    
    try:
        minio = MinIOService.from_env()
        
        # Check connection
        existing = minio.list_buckets()
        logger.info(f"Connected to MinIO. Existing buckets: {existing}")
        
        # Create buckets
        results = minio.ensure_buckets(buckets)
        
        # Summary
        created = [k for k, v in results.items() if v]
        existed = [k for k, v in results.items() if not v]
        
        if created:
            logger.success(f"Created buckets: {created}")
        if existed:
            logger.info(f"Already existed: {existed}")
        
        logger.success("✅ Storage setup complete!")
        return True
        
    except Exception as e:
        logger.error(f"Storage setup failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Initialize MinIO buckets for ETL framework"
    )
    parser.add_argument(
        "--buckets", 
        nargs="+", 
        default=DEFAULT_BUCKETS,
        help=f"Bucket names to create (default: {DEFAULT_BUCKETS})"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List existing buckets and exit"
    )
    
    args = parser.parse_args()
    
    if args.list:
        from etl.services.minio import MinIOService
        minio = MinIOService.from_env()
        buckets = minio.list_buckets()
        print("Existing buckets:")
        for b in buckets:
            print(f"  - {b}")
        return 0
    
    success = setup_storage(args.buckets)
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
