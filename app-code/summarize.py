import sys
from datetime import datetime

import ray
import ray.data as rd

from ioutils import *
from transformations import summarize

from loguru import logger

def run():
    ray.init(
        runtime_env={
            "env_vars": {
                "SSL_CERT_FILE": CA_PATH,
                "AWS_EC2_METADATA_DISABLED": "true",
                # "VLLM_USE_V1": "0",
            }
        }, 
    )

    news_location = get_hive_table_location(HMS_DB, "news")
    logger.info("Reading news from minio: {}", {"db": HMS_DB, "table": "news", "location": news_location})
    news = read_delta(news_location, storage_options=storage_opts_for_delta())
    logger.info("Read news dataset: {}", {
        "root": news_location,
        "sample": news.take(1),
        "count": news.count(),
        "time": datetime.now().isoformat(),
    })
    news = summarize(news)
    logger.info("Summarized news dataset: {}", {
        "root": news_location,
        "sample": news.take(1),
        "count": news.count(),
        "time": datetime.now().isoformat(),
    })
    news_path = f"{DELTA_ROOT}/news-summarized"
    write_delta_distributed(news, news_path, storage_options=storage_opts_for_delta())
    register_delta_table_in_hive(
        db=HMS_DB,
        table="news-summarized",
        location=news_path,
        columns=HiveMetastoreClient.hive_columns_from_arrow(arrow_schema_from_dataset(news)),
    )

    logger.info("Summarization complete: {}", {
        "root": DELTA_ROOT,
        "count": news.count(),
        "time": datetime.now().isoformat(),
    })


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("Summarization failed:", e, file=sys.stderr)
        raise
