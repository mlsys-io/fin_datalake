"""
Static Join Demo Pipeline

Reads two datasets, performs a distributed join, and writes to Delta Lake.
Demonstrates Milestone 3 (Static Data + Distributed Transform).
Imports are inside task methods for remote Ray execution.
"""
from typing import Dict, Any
from prefect import flow
from prefect_ray.task_runners import RayTaskRunner

# Only import lightweight base class at module level
from etl.core.base_task import BaseTask

# Load config (auto-loads .env via python-dotenv)
from etl.config import config


class JoinProcessTask(BaseTask):
    """
    Reads two datasets, performs a distributed join, and writes to Delta.
    """
    REQUIRED_DEPENDENCIES = ["ray", "pyarrow", "pandas"]
    
    def run(self, 
            orders_path: str, 
            customers_path: str, 
            output_uri: str,
            hive_config: Dict[str, Any] = None):
        # Heavy imports inside run() - executes on Ray worker
        from etl.io.sources.file import FileSource
        from etl.io.sinks.delta_lake import DeltaLakeSink
        from etl.services.hive import HiveMetastore
        
        print(f"[{self.name}] Starting Distributed Join Pipeline...")
        
        # 1. Define Sources
        src_orders = FileSource(paths=[orders_path], format="csv")
        src_cust = FileSource(paths=[customers_path], format="csv")
        
        # 2. Open Readers & Get Datasets
        with src_orders.open() as r_orders, src_cust.open() as r_cust:
            ds_orders = next(r_orders.read_batch())
            ds_cust = next(r_cust.read_batch())
            
            print(f"[{self.name}] Datasets initialized. Orders: {orders_path}, Customers: {customers_path}")
            
            # 3. Distributed Join (Ray Logic)
            print(f"[{self.name}] Executing join on 'customer_id'...")
            ds_joined = ds_orders.join(ds_cust, on="customer_id", how="inner")
            
            # 4. Write to Sink
            sink = DeltaLakeSink(
                uri=output_uri,
                mode="overwrite",
                hive_config=hive_config,
                hive_table_name="joined_orders_customers" if hive_config else None
            )
            
            print(f"[{self.name}] Writing result to {output_uri}...")
            with sink.open() as writer:
                writer.write_batch(ds_joined)
                
        print(f"[{self.name}] Join Pipeline Complete.")


@flow(name="Static Distributed Join Demo", task_runner=RayTaskRunner(address=config.RAY_ADDRESS))
def static_join_pipeline(
    orders_path: str = "mnt/data/orders/*.csv",
    customers_path: str = "mnt/data/customers/*.csv",
    output_root: str = "s3://delta-lake/cs4221/bronze-ray",
    hive_host: str = "localhost"
):
    output_uri = f"{output_root}/joined_orders"
    hive_conf = {"host": hive_host}
    
    task_instance = JoinProcessTask(name="Join Processor")
    task_instance.submit(orders_path, customers_path, output_uri, hive_conf)


if __name__ == "__main__":
    static_join_pipeline()
