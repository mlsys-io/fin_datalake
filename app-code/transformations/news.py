import ray.data as rd
from .helper import process_html_file, regex_extractor
from ioutils import MAX_CONCURRENCY

def transform_news_html(html_ds: rd.Dataset) -> rd.Dataset:
    return html_ds.rename_columns({"path": "_source_path"})\
        .add_column("symbol", regex_extractor(r"/News/([^/]+)/"), concurrency=MAX_CONCURRENCY)\
        .add_column("id", regex_extractor(r"/news/(\d+)\.html$", 'int64'), concurrency=MAX_CONCURRENCY)\
        .add_column("content", lambda df: df["text"].apply(process_html_file), concurrency=MAX_CONCURRENCY)\
        .drop_columns(["text", "_source_path"]) # We don't need path to the raw html as we already have the ID

def transform_news(json_ds: rd.Dataset, html_ds: rd.Dataset) -> rd.Dataset:
    return json_ds.rename_columns({"path": "_source_path"})\
        .add_column("symbol", regex_extractor(r"/News/([^/]+)/"), concurrency=MAX_CONCURRENCY)\
        .join(
            transform_news_html(html_ds), 
            join_type="left_outer",
            num_partitions=MAX_CONCURRENCY,
            on=("id", "symbol"), 
        )