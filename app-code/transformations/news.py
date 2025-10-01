from typing import Dict, List
import ray.data as rd
from .helper import process_html_file, regex_extractor
from ioutils import MAX_CONCURRENCY
from .llm import BaseModelHost, ModelHostSpec, inference_wrapper, VLLMModelHost
import pyarrow as pa

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

def summary_runner(host, batch: pa.StringArray, *, max_tokens: int = 256, temperature: float = 0.0, system: str = "Summarize the text concisely for a technical reader."):
    """Returns List[str] summaries aligned with `batch`."""
    messages: List[List[Dict[str, str]]] = [
        [
            {"role": "system", "content": system},
            {"role": "user", "content": t},
        ]
        for t in batch.to_pylist()
    ]
    prompts = host.tokenize(messages) 
    summaries = host.infer(prompts, max_tokens=max_tokens, temperature=temperature)
    return pa.array(summaries, pa.string())

def summarize(ds: rd.Dataset, model="Qwen/Qwen2.5-0.5B-Instruct") -> rd.Dataset:
    spec = ModelHostSpec(
        host_cls=VLLMModelHost,
        model_key=model,
        namespace="llm",
        num_cpus=8,
        num_gpus=0,
        init_kwargs=dict(
            model=model,
            dtype="bfloat16",
            tensor_parallel_size=1,
            engine_kwargs=dict(
                enable_chunked_prefill=True,
                max_num_batched_tokens=4096,
            ),
        ),
    )
    ds = ds.add_column(
        "processed_summary", 
        concurrency=MAX_CONCURRENCY, 
        **inference_wrapper(
            spec,
            runner_name="summary",
            runner_callable=summary_runner,
            preprocess=lambda batch: batch["summary"],   
            postprocess=lambda _, outs: outs,    
            batch_size=1024,
            batch_format="pyarrow",
            num_cpus=1,
            runner_kwargs=dict(max_tokens=5, temperature=0.3),
        ))
    return ds