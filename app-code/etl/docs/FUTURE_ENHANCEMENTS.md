# Future Enhancements

Tracking planned improvements and ideas for the ETL framework.

## High Priority

### `@io_safe` Decorator
Auto-route tasks with known conflicts to local execution.
```python
@io_safe  # Automatically uses .local() if needed
class DeltaWriteTask(BaseTask):
    pass
```

### Compatibility Checker
Runtime check for library conflicts before task execution.
```python
def check_compatibility(library: str) -> bool:
    """Check if library is safe for Ray workers."""
    pass
```

---

## Medium Priority

### Custom TaskRunner Support
Abstract TaskRunner interface for swapping Ray with custom engine.
```python
class CustomTaskRunner(BaseTaskRunner):
    def submit(self, task, *args):
        # Route to custom execution engine
        pass
```

### Delta Lake Ray Data Integration
Use ZY's `DeltaLakeSink` pattern within BaseTask for distributed Delta writes.

### Task Dependency Visualization
Generate DAG visualization from Prefect flows.

---

## Low Priority / Ideas

### Auto-retry with Local Fallback
If distributed execution fails with runtime error, automatically retry locally.

### Resource Estimation
Estimate CPU/memory needs based on input size.

### Caching Layer
Cache intermediate results in Redis/MinIO.

---

## Completed

- [x] `as_local_task()` method for I/O conflicts
- [x] `__call__()` clean API for distributed execution
- [x] `local()` clean API for local execution
- [x] `python-dotenv` configuration management
