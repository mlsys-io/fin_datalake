"""
Script to bulk-upload built-in policies to the S3 Hot-Reload bucket.
"""
import os
import boto3
from loguru import logger

def upload_seed_policies():
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:9000") # running outside cluster
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    
    s3 = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name="us-east-1"
    )
    
    bucket = "delta-lake"
    prefix = "overseer-policies"
    
    source_dir = os.path.join(os.path.dirname(__file__), "..", "overseer", "policies")
    
    uploaded = 0
    for file in os.listdir(source_dir):
        if file.endswith(".py") and file not in ["__init__.py", "base.py"]:
            local_path = os.path.join(source_dir, file)
            s3_key = f"{prefix}/{file}"
            try:
                s3.upload_file(local_path, bucket, s3_key)
                logger.info(f"Uploaded policy {file} to s3://{bucket}/{s3_key}")
                uploaded += 1
            except Exception as e:
                logger.error(f"Failed to upload {file}: {e}")
                
    logger.info(f"Seed complete. {uploaded} policies pushed to Lakehouse.")

if __name__ == "__main__":
    upload_seed_policies()
