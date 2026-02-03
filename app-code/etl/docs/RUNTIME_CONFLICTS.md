# Known Runtime Conflicts

This document tracks known runtime conflicts when using Ray as the distributed execution engine.

## Confirmed Conflicts

### Delta Lake (deltalake-python)
- **Library:** `deltalake`
- **Root Cause:** Tokio (Rust async runtime) conflicts with Ray's async executor
- **Error:** `TokioMultiThreadExecutor has crashed: RecvError`
- **Solution:** Use `DeltaLakeWriteTask` with `.local()` method

**Usage:**
```python
from etl.io import DeltaLakeWriteTask

# Create task
write_task = DeltaLakeWriteTask(
    name="Write to Bronze",
    uri="s3://delta-lake/bronze/table",
    mode="append"
)

# ✅ Correct - runs on driver, avoids Tokio crash
write_task.local(data)

# ⚠️ Will warn and may crash on Ray workers
write_task(data)
```

**Architecture:**
```
DeltaLakeSink      → Low-level config (no Prefect)
DeltaLakeWriter    → Implementation
DeltaLakeWriteTask → BaseTask wrapper (use this in pipelines)
```

## Potential Conflicts (Monitor)

### Polars (Lazy Evaluation)
- **Library:** `polars`
- **Risk:** Uses Tokio internally
- **Status:** Untested
- **Solution:** If crash occurs, use `task.local()`

### Milvus (pymilvus)
- **Library:** `pymilvus`
- **Risk:** gRPC async threading model
- **Status:** Untested  
- **Solution:** Initialize client inside task, monitor for hangs

### Confluent Kafka
- **Library:** `confluent-kafka`
- **Risk:** librdkafka thread handling
- **Status:** Generally works, but consumer groups may have issues
- **Solution:** Keep consumer lifecycle within single task

### GPU Libraries (PyTorch/TensorFlow)
- **Library:** `torch`, `tensorflow`
- **Risk:** GPU memory management across workers
- **Status:** Works with proper resource allocation
- **Solution:** Use Ray's GPU resource management: `@ray.remote(num_gpus=1)`

## Safe Libraries (No Known Conflicts)

| Library | Notes |
|---------|-------|
| `pandas` | Pure Python/C, no async runtime |
| `pyarrow` | C++, no async runtime |
| `psycopg2` | Synchronous PostgreSQL driver |
| `requests` | Synchronous HTTP |
| `thrift` | Python Thrift client |
| `langchain` | Generally safe, depends on underlying LLM client |

## Best Practices

1. **Initialize connections inside tasks** - Not at module level
2. **Test new libraries on Ray workers** - Before production use
3. **Use `task.local()` for I/O** - When in doubt
4. **Check for Rust/Tokio dependencies** - `pip show <lib>` for dependencies
