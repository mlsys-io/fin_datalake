"""
ETL I/O Module - Sources, Sinks, and I/O Tasks.
"""
from etl.io.base import DataSource, DataSink, DataReader, DataWriter

try:
    from etl.io.tasks import DeltaLakeWriteTask
    from etl.io.tasks import RisingWaveWriteTask
except ImportError:
    DeltaLakeWriteTask = None
    RisingWaveWriteTask = None

__all__ = [
    # Base classes
    "DataSource",
    "DataSink", 
    "DataReader",
    "DataWriter",
    # Tasks
    "DeltaLakeWriteTask",
    "RisingWaveWriteTask",
]
