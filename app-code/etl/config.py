"""
ETL Framework Configuration

Centralized configuration using python-dotenv.
Automatically loads .env file from project root.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Find and load .env file from project root (zdb_deployment/)
# Search upward from this file's location
def _find_env_file() -> Path | None:
    """Find .env file by searching upward from current directory."""
    # Start from app-code directory
    current = Path(__file__).parent.parent
    
    # Check app-code directory first
    if (current / ".env").exists():
        return current / ".env"
    
    # Then check parent (zdb_deployment)
    parent = current.parent
    if (parent / ".env").exists():
        return parent / ".env"
    
    # Check current working directory as fallback
    cwd = Path.cwd()
    if (cwd / ".env").exists():
        return cwd / ".env"
    
    return None


# Load .env file on module import
_env_file = _find_env_file()
if _env_file:
    load_dotenv(_env_file)


class Config:
    """
    Central configuration class.
    All values are read from environment variables with sensible defaults.
    """
    
    # K8s / Network
    NODE_IP: str = os.environ.get("NODE_IP", "localhost")
    
    # Ray
    RAY_ADDRESS: str = os.environ.get("RAY_ADDRESS", "auto")
    RAY_DASHBOARD_URL: str = os.environ.get("RAY_DASHBOARD_URL", "")
    
    # Prefect
    PREFECT_API_URL: str = os.environ.get("PREFECT_API_URL", "")
    
    # S3 / MinIO
    AWS_ACCESS_KEY_ID: str = os.environ.get("AWS_ACCESS_KEY_ID", "")
    AWS_SECRET_ACCESS_KEY: str = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    AWS_ENDPOINT_URL: str = os.environ.get("AWS_ENDPOINT_URL", "")
    AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")
    
    # S3 / MinIO TLS Certificate
    CA_PATH: str = os.environ.get("CA_PATH", "/opt/certs/public.crt")
    
    # Delta Lake
    DELTA_ROOT: str = os.environ.get("DELTA_ROOT", "s3://delta-lake/bronze")
    
    # TimescaleDB
    TSDB_HOST: str = os.environ.get("TSDB_HOST", "")
    TSDB_PORT: int = int(os.environ.get("TSDB_PORT", "5432"))
    TSDB_USER: str = os.environ.get("TSDB_USER", "app")
    TSDB_PASSWORD: str = os.environ.get("TSDB_PASSWORD", "")
    TSDB_DATABASE: str = os.environ.get("TSDB_DATABASE", "app")
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
    
    # Demo Sources
    API_URL: str = os.environ.get("API_URL", "")
    WEBSOCKET_URL: str = os.environ.get("WEBSOCKET_URL", "")
    STATIC_URL: str = os.environ.get("STATIC_URL", "")
    
    # Hive Metastore
    HIVE_HOST: str = os.environ.get("HIVE_HOST", "")
    HIVE_PORT: int = int(os.environ.get("HIVE_PORT", "9083"))
    
    # RisingWave
    RISINGWAVE_HOST: str = os.environ.get("RISINGWAVE_HOST", "")
    RISINGWAVE_PORT: int = int(os.environ.get("RISINGWAVE_PORT", "4566"))
    
    # Redis
    REDIS_URL: str = os.environ.get("OVERSEER_REDIS_URL", "")
    
    # Data Paths
    INPUT_PATH: str = os.environ.get("INPUT_PATH", "/mnt/data")

    # Concurrency
    MAX_CONCURRENCY: int = int(os.environ.get("MAX_CONCURRENCY", "10"))
    
    @classmethod
    def reload(cls):
        """Reload configuration from environment."""
        if _env_file:
            load_dotenv(_env_file, override=True)
            
        # Re-read all values
        cls.NODE_IP = os.environ.get("NODE_IP", "localhost")
        cls.RAY_ADDRESS = os.environ.get("RAY_ADDRESS", "auto")
        cls.RAY_DASHBOARD_URL = os.environ.get("RAY_DASHBOARD_URL", "")
        cls.PREFECT_API_URL = os.environ.get("PREFECT_API_URL", "")
        
        cls.AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "")
        cls.AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        cls.AWS_ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "")
        cls.AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
        cls.CA_PATH = os.environ.get("CA_PATH", "/opt/certs/public.crt")
        cls.DELTA_ROOT = os.environ.get("DELTA_ROOT", "s3://delta-lake/bronze")
        
        cls.TSDB_HOST = os.environ.get("TSDB_HOST", "")
        cls.TSDB_PORT = int(os.environ.get("TSDB_PORT", "5432"))
        cls.TSDB_USER = os.environ.get("TSDB_USER", "app")
        cls.TSDB_PASSWORD = os.environ.get("TSDB_PASSWORD", "")
        cls.TSDB_DATABASE = os.environ.get("TSDB_DATABASE", "app")
        
        cls.KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
        cls.API_URL = os.environ.get("API_URL", "")
        cls.WEBSOCKET_URL = os.environ.get("WEBSOCKET_URL", "")
        cls.STATIC_URL = os.environ.get("STATIC_URL", "")
        
        cls.HIVE_HOST = os.environ.get("HIVE_HOST", "")
        cls.HIVE_PORT = int(os.environ.get("HIVE_PORT", "9083"))
        cls.RISINGWAVE_HOST = os.environ.get("RISINGWAVE_HOST", "")
        cls.RISINGWAVE_PORT = int(os.environ.get("RISINGWAVE_PORT", "4566"))
        
        cls.REDIS_URL = os.environ.get("OVERSEER_REDIS_URL", "")
        cls.INPUT_PATH = os.environ.get("INPUT_PATH", "/mnt/data")
        cls.MAX_CONCURRENCY = int(os.environ.get("MAX_CONCURRENCY", "10"))


# Singleton instance for easy import
config = Config()
