import pandas as pd
from deltalake import DeltaTable
from etl.config import config
import os

def inspect_results(table_uri: str = "s3://delta-lake/bronze/kafka-events"):
    print(f"🔍 Inspecting Delta Table: {table_uri}")
    
    # Configure storage options for MinIO
    storage_options = {
        "AWS_ACCESS_KEY_ID": config.AWS_ACCESS_KEY_ID,
        "AWS_SECRET_ACCESS_KEY": config.AWS_SECRET_ACCESS_KEY,
        "AWS_ENDPOINT_URL": config.AWS_ENDPOINT_URL,
        "AWS_REGION": config.AWS_REGION,
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    
    # Check for SSL cert
    if os.path.exists(config.CA_PATH):
        os.environ["SSL_CERT_FILE"] = config.CA_PATH
        print(f"✅ Loaded SSL Cert: {config.CA_PATH}")

    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        df = dt.to_pandas()
        
        print("\n📊 Table Summary:")
        print(f"Total Rows: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        
        print("\n🕒 Latest 5 Records:")
        print(df.tail(5).to_string())
        
    except Exception as e:
        print(f"❌ Error reading Delta table: {e}")

if __name__ == "__main__":
    inspect_results()
