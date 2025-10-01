import sys
from glob import glob
from datetime import datetime
import pyarrow as pa
from pyarrow import csv, json

import ray
import ray.data as rd

from ioutils import *
from transformations import transform_ohlc, transform_news, transform_sec

def run():
    ray.init(
        runtime_env={
            "env_vars": {
                "SSL_CERT_FILE": CA_PATH,
                "AWS_EC2_METADATA_DISABLED": "true",
            }
        }, 
    )

    ohlc = rd.read_csv(
        glob(f"{INPUT_PATH}/OHLC/*/*.gz"),
        include_paths=True,
        convert_options=csv.ConvertOptions(column_types={
                "timestamp": pa.int64(),
                "open": pa.float64(),
                "high": pa.float64(),
                "low": pa.float64(),
                "close": pa.float64(),
                "volume": pa.float64(),
            }
        )
    )
    ohlc = transform_ohlc(ohlc)
    ohlc_path = f"{DELTA_ROOT}/ohlc"
    write_delta_distributed(ohlc, ohlc_path, storage_options=storage_opts_for_delta())
    register_delta_table_in_hive(
        db=HMS_DB,
        table="ohlc",
        location=ohlc_path,
        columns=HiveMetastoreClient.hive_columns_from_arrow(arrow_schema_from_dataset(ohlc)),
    )

    news_json = rd.read_json(
        glob(f"{INPUT_PATH}/News/*/news.json"), 
        include_paths=True,
        concurrency=MAX_CONCURRENCY, 
        parse_options=pa.json.ParseOptions(explicit_schema=pa.schema([
            pa.field("category", pa.string()),
            pa.field("datetime", pa.int64()),
            pa.field("headline", pa.string()),
            pa.field("id", pa.int64()),
            pa.field("image", pa.string()),
            pa.field("related", pa.string()),
            pa.field("source", pa.string()),
            pa.field("summary", pa.string()),
            pa.field("url", pa.string()),
        ]))
    )
    news_html = rd.read_text(
        glob(f"{INPUT_PATH}/News/*/news/*.html"), 
        include_paths=True,
        concurrency=MAX_CONCURRENCY,
    )
    news = transform_news(news_json, news_html)
    news_path = f"{DELTA_ROOT}/news"
    write_delta_distributed(news, news_path, storage_options=storage_opts_for_delta())
    register_delta_table_in_hive(
        db=HMS_DB,
        table="news",
        location=news_path,
        columns=HiveMetastoreClient.hive_columns_from_arrow(arrow_schema_from_dataset(news)),
    )

    sec = rd.read_text(
        glob(f"{INPUT_PATH}/SEC-Filings/*/10-Q/*/full-submission.txt"), 
        include_paths=True,
        concurrency=MAX_CONCURRENCY,
    )
    sec = transform_sec(sec)
    sec_path = f"{DELTA_ROOT}/sec_filings"
    write_delta_distributed(sec, sec_path, storage_options=storage_opts_for_delta())
    register_delta_table_in_hive(
        db=HMS_DB,
        table="sec_filings",
        location=sec_path,
        columns=HiveMetastoreClient.hive_columns_from_arrow(arrow_schema_from_dataset(sec)),
    )

    print("Basic ETL complete:", {
        "root": DELTA_ROOT,
        "time": datetime.now().isoformat(),
    })


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("ETL failed:", e, file=sys.stderr)
        raise
