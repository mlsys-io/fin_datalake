import sys
from glob import glob
from datetime import datetime
import pyarrow as pa
from pyarrow import csv

import ray
import ray.data as rd

from ioutils import *

def add_audit_cols(ds: rd.Dataset) -> rd.Dataset:
    def _add_cols(tbl: pa.Table) -> pa.Table:
        out = tbl
        assert "path" in out.schema.names, "Expected 'path' column to exist"
        out = out.append_column("_source_path", out["path"])
        out = out.drop_columns(["path"])
        return out

    return ds.map_batches(_add_cols, batch_format="pyarrow", concurrency=MAX_CONCURRENCY)

def rename_text_to_value(ds: rd.Dataset) -> rd.Dataset:
    def _rename(tbl: pa.Table) -> pa.Table:
        if "text" in tbl.schema.names:
            i = tbl.schema.get_field_index("text")
            tbl = tbl.set_column(i, "value", tbl["text"])
        return tbl
    return ds.map_batches(_rename, batch_format="pyarrow", concurrency=MAX_CONCURRENCY)

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
        parallelism=MAX_CONCURRENCY, 
    )
    ohlc = add_audit_cols(ohlc)
    ohlc_path = f"{DELTA_ROOT}/ohlc"
    write_delta_distributed(ohlc, ohlc_path, storage_options=storage_opts_for_delta())
    register_delta_table_in_hive(
        db=HMS_DB,
        table="ohlc",
        location=ohlc_path,
        columns=HiveMetastoreClient.hive_columns_from_arrow(ohlc.schema()),
    )

    news_json = rd.read_json(
        glob(f"{INPUT_PATH}/News/*/news.json"), 
        include_paths=True,
        concurrency=MAX_CONCURRENCY, 
    )
    news_json = add_audit_cols(news_json)
    news_json_path = f"{DELTA_ROOT}/news_json"
    write_delta_distributed(news_json, news_json_path, storage_options=storage_opts_for_delta())
    register_delta_table_in_hive(
        db=HMS_DB,
        table="news_json",
        location=news_json_path,
        columns=HiveMetastoreClient.hive_columns_from_arrow(news_json.schema()),
    )

    rd.DataContext.get_current().log_internal_stack_trace_to_stdout = True
    news_html = rd.read_text(
        glob(f"{INPUT_PATH}/News/*/news/*.html"), 
        include_paths=True,
        concurrency=MAX_CONCURRENCY,
    )
    news_html = rename_text_to_value(add_audit_cols(news_html))
    news_html_path = f"{DELTA_ROOT}/news_html"
    write_delta_distributed(news_html, news_html_path, storage_options=storage_opts_for_delta())
    register_delta_table_in_hive(
        db=HMS_DB,
        table="news_html",
        location=news_html_path,
        columns=HiveMetastoreClient.hive_columns_from_arrow(news_html.schema()),
    )

    sec = rd.read_text(
        glob(f"{INPUT_PATH}/SEC-Filings/*/10-Q/*/full-submission.txt"), 
        include_paths=True,
        concurrency=MAX_CONCURRENCY,
    )
    sec = rename_text_to_value(add_audit_cols(sec))
    sec_path = f"{DELTA_ROOT}/sec_filings"
    write_delta_distributed(sec, sec_path, storage_options=storage_opts_for_delta())
    register_delta_table_in_hive(
        db=HMS_DB,
        table="sec_filings",
        location=sec_path,
        columns=HiveMetastoreClient.hive_columns_from_arrow(sec.schema()),
    )

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
