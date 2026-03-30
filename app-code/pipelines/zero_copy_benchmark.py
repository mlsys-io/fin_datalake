import time
import json
import asyncio
import os
import ray
import ray.serve as serve
import pandas as pd
import pyarrow as pa
from loguru import logger
import tracemalloc

from sample_agents.strategy_agent import StrategyAgent
from etl.agents.hub import get_hub

def run_zero_copy_benchmark():
    """
    Quantifies the advantage of Ray Shared Memory (Zero-Copy) 
    vs Traditional JSON Serialization.
    """
    logger.info("=== 🏎️  ZERO-COPY VS SERIALIZATION BENCHMARK ===")
    
    if not ray.is_initialized():
        ray.init(ignore_reinit_error=True)
        
    # Deploy a fresh StrategyAgent for clean measurements
    agent_bench_name = "BenchmarkAgent-1"
    StrategyAgent.deploy(name=agent_bench_name)
    agent = serve.get_app_handle(agent_bench_name)
    
    # Sizes to test: 1K, 10K, 100K, 1M rows
    sizes = [1_000, 10_000, 100_000, 1_000_000]
    results = []

    for size in sizes:
        logger.info(f"\n[Size: {size:,} rows]")
        
        # 1. Generate Synthetic Data (OHLC + Signal info)
        df = pd.DataFrame({
            'close': [60000.0 + i % 100 for i in range(size)],
            'volume': [1.5 for _ in range(size)],
            'timestamp': [time.time() + i for i in range(size)]
        })
        table = pa.Table.from_pandas(df)
        
        # --- PATH A: SERIALIZED (Traditional) ---
        logger.debug("Measuring Serialized Path...")
        tracemalloc.start()
        t0 = time.perf_counter()
        
        # Simulate serialization/deserialization cost
        json_data = df.to_json() 
        _ = pd.read_json(json_data) 
        
        # Actual request — serialize to dict and send via Serve handle
        _ = asyncio.run(agent.ask({"symbol": "BTC", "ohlc_data": df.to_dict(orient="records")}))
        
        t_serial = (time.perf_counter() - t0) * 1000
        _, peak_serial = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        # --- PATH B: ZERO-COPY (Our System) ---
        logger.debug("Measuring Zero-Copy Path...")
        tracemalloc.start()
        t1 = time.perf_counter()
        
        # Put into Ray Object Store
        ref = ray.put(table)
        
        # Pass ObjectRef directly - Agent calls ray.get(ref) inside
        _ = asyncio.run(agent.ask({"symbol": "BTC", "data_ref": ref}))
        
        t_zero = (time.perf_counter() - t1) * 1000
        _, peak_zero = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        
        # --- ANALYSIS ---
        speedup = t_serial / t_zero
        mem_saved = (peak_serial - peak_zero) / (1024 * 1024) # MB
        
        logger.success(f"  Serialized: {t_serial:.2f}ms | Peak Mem: {peak_serial/(1024*1024):.2f}MB")
        logger.success(f"  Zero-Copy:   {t_zero:.2f}ms | Peak Mem: {peak_zero/(1024*1024):.2f}MB")
        logger.success(f"  🚀 Speedup: {speedup:.1f}x | Mem Saved: {mem_saved:.2f}MB")
        
        results.append({
            "size": size,
            "serialized_ms": t_serial,
            "zero_copy_ms": t_zero,
            "speedup": speedup,
            "mem_saved_mb": mem_saved
        })

    # Final Summary Table
    print("\n" + "="*60)
    print(f"{'Rows':<12} | {'Serialized':>12} | {'Zero-Copy':>12} | {'Speedup':>10}")
    print("-" * 60)
    for r in results:
        print(f"{r['size']:<12,} | {r['serialized_ms']:>10.1f} ms | {r['zero_copy_ms']:>10.1f} ms | {r['speedup']:>8.1f}x")
    print("="*60 + "\n")
    
    return results

if __name__ == "__main__":
    try:
        run_zero_copy_benchmark()
    except KeyboardInterrupt:
        logger.info("Benchmark interrupted.")
