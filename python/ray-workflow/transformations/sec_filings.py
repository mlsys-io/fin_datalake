import ray.data as rd
from .helper import process_sec_file, regex_extractor
from ioutils import MAX_CONCURRENCY

def transform_sec(ds: rd.Dataset) -> rd.Dataset:
    return ds.rename_columns({"path": "_source_path"})\
        .add_column("symbol", regex_extractor(r"/SEC-Filings/([^/]+)/"), concurrency=MAX_CONCURRENCY)\
        .add_column("content", lambda df: df["text"].apply(process_sec_file), concurrency=MAX_CONCURRENCY)\
        .drop_columns(["text"])