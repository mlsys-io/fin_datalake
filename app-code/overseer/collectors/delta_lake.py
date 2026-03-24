"""
DeltaLakeCollector — Reads metadata from a Delta Lake table to monitor data freshness.
"""
from typing import Any
import os
import time
import asyncio
from loguru import logger
from deltalake import DeltaTable

from overseer.collectors.base import BaseCollector
from overseer.models import ServiceEndpoint, ServiceMetrics
from overseer.redis_utils import get_redis_client

class DeltaLakeCollector(BaseCollector):
    
    def __init__(self, endpoint: ServiceEndpoint):
        super().__init__(endpoint)
        self.redis = get_redis_client()
            
    async def collect(self) -> ServiceMetrics:
        if not self.redis:
            return ServiceMetrics(
                service=self.endpoint.name, healthy=False,
                error="Redis not configured — cannot discover Delta Lake targets",
            )

        try:
            targets = await self.redis.hgetall("overseer:targets:delta_lake")
            if not targets:
                return ServiceMetrics(
                    service=self.endpoint.name,
                    healthy=True,
                    data={"tables": {}}
                )
        except Exception as e:
            logger.error(f"DeltaLakeCollector failed to reach Redis: {e}")
            return ServiceMetrics(service=self.endpoint.name, healthy=False, error=str(e))

        # Read credentials from environment (consistent with s3_sync.py)
        endpoint_url = os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000")
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

        def _fetch_history(uri: str, name: str):
            try:
                dt = DeltaTable(
                    uri,
                    storage_options={
                        "AWS_ENDPOINT_URL": endpoint_url,
                        "AWS_ACCESS_KEY_ID": access_key,
                        "AWS_SECRET_ACCESS_KEY": secret_key,
                        "AWS_ALLOW_HTTP": "true",
                        "AWS_REGION": "us-east-1",
                        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
                    }
                )
                history = dt.history(limit=1)
                if not history:
                    return name, {"latest_commit_time": 0, "version": dt.version()}
                
                commit_ts = history[0].get("timestamp", 0) / 1000.0
                return name, {"latest_commit_time": commit_ts, "version": dt.version()}
            except Exception as e:
                return name, {"error": str(e)}

        try:
            tasks = [
                asyncio.to_thread(_fetch_history, uri, name)
                for name, uri in targets.items()
            ]
            results = await asyncio.gather(*tasks)
            
            tables_data = {n: d for n, d in results}

            return ServiceMetrics(
                service=self.endpoint.name,
                healthy=True,
                data={"tables": tables_data}
            )
        except Exception as e:
            logger.error(f"DeltaLakeCollector failed gathering histories: {e}")
            return ServiceMetrics(
                service=self.endpoint.name,
                healthy=False,
                error=str(e)
            )
