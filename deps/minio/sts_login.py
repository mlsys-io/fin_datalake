#!/usr/bin/env python3
import argparse
import requests
import xml.etree.ElementTree as ET
import boto3
from botocore.client import Config
import sys
import os

def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Connect to MinIO using mTLS and Exchange Certs for STS Credentials."
    )
    
    # Connection Args
    parser.add_argument("--url", default="https://localhost:9000", 
                        help="MinIO Server URL (default: https://localhost:9000)")
    
    # Certificate Args
    parser.add_argument("--cert", default="client-certs/public.crt",
                        help="Path to Client Public Certificate (default: client-certs/public.crt)")
    parser.add_argument("--key", default="client-certs/private.key",
                        help="Path to Client Private Key (default: client-certs/private.key)")
    parser.add_argument("--ca", default="CAs/minio-ca.crt",
                        help="Path to CA Certificate (default: CAs/minio-ca.crt)")
    
    # Operational Args
    parser.add_argument("--bucket", help="Optional: Specific bucket to list objects from")

    return parser.parse_args()

def get_sts_credentials(args):
    """
    Exchanges the X.509 Client Certificate for temporary S3 Credentials 
    via the MinIO STS API (AssumeRoleWithCertificate).
    """
    print(f"1. Requesting STS credentials via mTLS from {args.url}...")
    
    # Check if files exist before failing safely
    for fpath, name in [(args.cert, "Cert"), (args.key, "Key"), (args.ca, "CA")]:
        if not os.path.exists(fpath):
            print(f"Error: {name} file not found at: {fpath}")
            sys.exit(1)

    params = {
        "Action": "AssumeRoleWithCertificate",
        "Version": "2011-06-15",
        "DurationSeconds": 3600 # 1 Hour
    }

    try:
        # The mTLS magic happens here:
        response = requests.post(
            args.url,
            params=params,
            cert=(args.cert, args.key),
            verify=args.ca
        )
        response.raise_for_status()
        
        # Parse XML Response
        root = ET.fromstring(response.content)
        ns = {'sts': 'https://sts.amazonaws.com/doc/2011-06-15/'}
        
        creds = root.find('.//sts:Credentials', ns)
        if creds is None:
            print("Error: Credentials missing from STS response.")
            print(response.text)
            sys.exit(1)

        return {
            'access_key': creds.find('sts:AccessKeyId', ns).text,
            'secret_key': creds.find('sts:SecretAccessKey', ns).text,
            'session_token': creds.find('sts:SessionToken', ns).text
        }

    except Exception as e:
        print(f"STS Exchange Failed: {e}")
        sys.exit(1)

def connect_s3(args, creds):
    """
    Uses the exchanged credentials to perform an S3 operation via Boto3.
    """
    print("\n2. Credentials received. Initializing Boto3 client...")
    
    s3 = boto3.client('s3',
        endpoint_url=args.url,
        aws_access_key_id=creds['access_key'],
        aws_secret_access_key=creds['secret_key'],
        aws_session_token=creds['session_token'],
        config=Config(signature_version='s3v4'),
        verify=args.ca  # Important: Verify server TLS using the same CA
    )

    try:
        print("   Checking connectivity...")
        response = s3.list_buckets()
        
        print("\nSUCCESS: Connection established & Authorized!")
        print(f"User Identity: {response.get('Owner', {}).get('DisplayName', 'Unknown')}")
        
        print("\nBuckets:")
        for bucket in response.get('Buckets', []):
            print(f" - {bucket['Name']}")
            
        # Optional: List objects if a bucket was specified
        if args.bucket:
            print(f"\nListing objects in '{args.bucket}':")
            objs = s3.list_objects_v2(Bucket=args.bucket)
            for obj in objs.get('Contents', []):
                print(f"   - {obj['Key']} ({obj['Size']} bytes)")

    except Exception as e:
        print("\nFAILURE: S3 Operation Failed.")
        print(f"Error: {e}")

if __name__ == "__main__":
    args = parse_arguments()
    sts_creds = get_sts_credentials(args)
    connect_s3(args, sts_creds)