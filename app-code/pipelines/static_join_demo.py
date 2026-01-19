import os
import ray.data as rd
from prefect import flow
from prefect_ray.task_runners import RayTaskRunner
from typing import Dict, Any

from etl.core.base_task import BaseTask
from etl.io.sources.file import FileSource
from etl.io.sinks.delta_lake import DeltaLakeSink
from etl.services.hive import HiveMetastore

class JoinProcessTask(BaseTask):
    """
    Reads two datasets, performs a distributed join, and writes to Delta.
    Demonstrates Milestone 3 (Static Data + Distributed Transform).
    """
    REQUIRED_DEPENDENCIES = ["ray", "pyarrow", "pandas"]
    
    def run(self, 
            orders_path: str, 
            customers_path: str, 
            output_uri: str,
            hive_config: Dict[str, Any] = None):
        
        print(f"[{self.name}] Starting Distributed Join Pipeline...")
        
        # 1. Define Sources
        # We rely on Ray's lazy loading. The 'FileSource' just gives us the config.
        # But our FileReader yields a "Dataset" object which is exactly what we need for joins.
        
        src_orders = FileSource(paths=[orders_path], format="csv")
        src_cust = FileSource(paths=[customers_path], format="csv")
        
        # 2. Open Readers & Get Datasets
        # Since reading is lazy, this is cheap.
        with src_orders.open() as r_orders, src_cust.open() as r_cust:
            ds_orders = next(r_orders.read_batch())
            ds_cust = next(r_cust.read_batch())
            
            print(f"[{self.name}] Datasets initialized. Orders: {orders_path}, Customers: {customers_path}")
            
            # 3. Distributed Join (Ray Logic)
            # Assuming both have 'customer_id' column
            print(f"[{self.name}] Executing join on 'customer_id'...")
            ds_joined = ds_orders.join(ds_cust, on="customer_id", how="inner")
            
            # 4. Write to Sink
            sink = DeltaLakeSink(
                uri=output_uri,
                mode="overwrite",
                hive_metastore=HiveMetastore(**hive_config) if hive_config else None,
                hive_table_name="joined_orders_customers" if hive_config else None
            )
            
            print(f"[{self.name}] Writing result to {output_uri}...")
            # We can write the Ray DS directly to the sink's batch writer
            # The DeltaLakeSink is smart enough to handle 'ray.data.Dataset'
            with sink.open() as writer:
                writer.write_batch(ds_joined)
                
        print(f"[{self.name}] Join Pipeline Complete.")

@flow(name="Static Distributed Join Demo", task_runner=RayTaskRunner)
def static_join_pipeline(
    orders_path: str = "mnt/data/orders/*.csv",
    customers_path: str = "mnt/data/customers/*.csv",
    output_root: str = "s3://delta-lake/cs4221/bronze-ray",
    hive_host: str = "localhost"
):
    output_uri = f"{output_root}/joined_orders"
    hive_conf = {"host": hive_host}
    
    task_instance = JoinProcessTask(name="Join Processor")
    task_instance.as_task().submit(orders_path, customers_path, output_uri, hive_conf)

if __name__ == "__main__":
    # Dummy Main for testing
    static_join_pipeline()
