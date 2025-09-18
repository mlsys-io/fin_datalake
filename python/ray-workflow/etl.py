import sys
from glob import glob
from datetime import datetime
import pandas as pd
import pyarrow as pa
from pyarrow import csv

import ray
import ray.data as rd

from ioutils import *

def add_audit_cols(ds: rd.Dataset) -> rd.Dataset:
    def _add_cols(pdf: pd.DataFrame) -> pd.DataFrame:
        src = pdf["path"] if "path" in pdf.columns else ""
        pdf["_source_path"] = src
        return pdf
    ds = ds.map_batches(_add_cols, batch_format="pandas")
    if "path" in ds.schema().names:
        ds = ds.drop_columns(["path"])
    return ds

def rename_text_to_value(ds: rd.Dataset) -> rd.Dataset:
    if "text" in ds.schema().names:
        return ds.map_batches(lambda pdf: pdf.rename(columns={"text": "value"}), batch_format="pandas")
    return ds

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
        ),
    )
    ohlc = add_audit_cols(ohlc)
    write_delta_distributed(ohlc, f"{DELTA_ROOT}/ohlc", storage_options=storage_opts_for_delta())

    news_json = rd.read_json(
        glob(f"{INPUT_PATH}/News/*/news.json"), 
        include_paths=True,
    )
    news_json = add_audit_cols(news_json)
    write_delta_distributed(news_json, f"{DELTA_ROOT}/news_json", storage_options=storage_opts_for_delta())

    news_html = rd.read_text(glob(f"{INPUT_PATH}/News/*/news/*.html"), include_paths=True)
    news_html = rename_text_to_value(add_audit_cols(news_html))
    write_delta_distributed(news_html, f"{DELTA_ROOT}/news_html", storage_options=storage_opts_for_delta())

    sec = rd.read_text(
        glob(f"{INPUT_PATH}/SEC-Filings/*/10-Q/*/full-submission.txt"), 
        include_paths=True
    )
    sec = rename_text_to_value(add_audit_cols(sec))
    write_delta_distributed(sec, f"{DELTA_ROOT}/sec_filings", storage_options=storage_opts_for_delta())

    print("Bronze re-store complete:", {
        "root": DELTA_ROOT,
        "time": datetime.now().isoformat(),
    })


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("ETL failed:", e, file=sys.stderr)
        raise
