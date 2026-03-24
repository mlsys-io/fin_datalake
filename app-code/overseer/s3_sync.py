"""
S3 Sync Utility — Downloads custom policies from MinIO/S3.
"""
import os
import boto3
from urllib.parse import urlparse
from loguru import logger

def sync_policies(s3_uri: str, local_dir: str):
    """
    Downloads all .py files from the given S3 URI to the local directory.
    Assumes MinIO credentials are in environment or uses defaults for local MinIO.
    """
    if not s3_uri.startswith("s3://"):
        logger.warning(f"Invalid S3 URI for policies: {s3_uri}")
        return

    parsed = urlparse(s3_uri)
    bucket_name = parsed.netloc
    prefix = parsed.path.lstrip('/')

    os.makedirs(local_dir, exist_ok=True)

    endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    try:
        s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name="us-east-1"
        )
        
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        s3_files = set()
        downloaded: int = 0
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.py'):
                    filename = os.path.basename(key)
                    s3_files.add(filename)
                    local_file = os.path.join(local_dir, filename)
                    s3.download_file(bucket_name, key, local_file)
                    downloaded += 1

        # Clean up local files that are no longer in S3
        removed: int = 0
        for local_f in os.listdir(local_dir):
            if local_f.endswith('.py') and local_f not in s3_files:
                os.remove(os.path.join(local_dir, local_f))
                removed += 1
                logger.info(f"Removed custom policy {local_f} because it was deleted from S3.")
                
        if downloaded > 0 or removed > 0:
            logger.info(f"S3 Sync: {downloaded} downloaded, {removed} removed from {s3_uri}")

    except Exception as e:
        logger.error(f"Failed to sync policies from S3 ({s3_uri}): {e}")
