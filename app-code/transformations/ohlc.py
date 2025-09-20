import ray.data as rd
from .helper import regex_extractor
from ioutils import MAX_CONCURRENCY

def transform_ohlc(ds: rd.Dataset) -> rd.Dataset:
    return ds.rename_columns({"path": "_source_path"})\
        .add_column(
            "symbol", 
            regex_extractor(r"/OHLC/([^/]+)/"), 
            concurrency=MAX_CONCURRENCY
        )