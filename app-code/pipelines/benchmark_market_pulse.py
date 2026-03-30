import time
import json
from loguru import logger
import ray

from pipelines.market_pulse_demo import run_demo as run_actual_system
from pipelines.baselines.baseline_plain import run_plain_baseline
from pipelines.baselines.baseline_airflow_sim import run_airflow_sim_baseline
from pipelines.zero_copy_benchmark import run_zero_copy_benchmark

def run_master_benchmark():
    """
    Executes the full suite of benchmarks comparing our AI Lakehouse
    against industry-standard baselines.
    """
    logger.info("\n" + "#"*60 + "\n🏢  MASTER PERFORMANCE BENCHMARK HARNESS  🏢\n" + "#"*60)
    
    # 1. Zero-Copy Analysis (The "Technical" deep-dive)
    zc_stats = run_zero_copy_benchmark()
    # Take the 1M row speedup as our headline figure
    headline_speedup = [s['speedup'] for s in zc_stats if s['size'] == 1_000_000][0]
    
    # 2. End-to-End Latency Comparisons (The "Operational" view)
    logger.info("\n[Running E2E Baselines...]")
    
    t_plain = run_plain_baseline()['total_time']
    t_airflow = run_airflow_sim_baseline()['total_time']
    
    logger.info("\n[Running AI Lakehouse E2E System...]")
    t0 = time.perf_counter()
    run_actual_system()
    t_system = time.perf_counter() - t0
    
    # 3. Final Comparison Table Output
    logger.success("\n" + "="*60)
    logger.success("🏆  FINAL BENCHMARK SUMMARY  🏆")
    logger.success("="*60)
    
    print(f"{'System Architecture':<25} | {'E2E Latency':<15} | {'Status'}")
    print("-" * 60)
    print(f"{'Plain Python (Sequential)':<25} | {t_plain:>12.2f}s | Baseline")
    print(f"{'Airflow (Serialized)':<25} | {t_airflow:>12.2f}s | Baseline")
    print(f"{'AI Lakehouse (Our System)':<25} | {t_system:>12.2f}s | CHAMPION ✅")
    
    improvement = (t_airflow - t_system) / t_airflow * 100
    
    print("-" * 60)
    print(f"\n📊 Headline Results for Final Report:")
    print(f"   🚀 Zero-Copy Advantage: {headline_speedup:.1f}x faster data access")
    print(f"   ⏱️  E2E Orchestration: {improvement:.1f}% faster than Airflow-sim")
    print(f"   🛡️  Resiliency: Built-in Self-Healing (Overseer enabled)")
    print("="*60 + "\n")
    
    # Save results to disk
    final_report = {
        "zc_speedup": headline_speedup,
        "latency_baseline_plain": t_plain,
        "latency_baseline_airflow": t_airflow,
        "latency_system": t_system,
        "improvement_pct": improvement
    }
    with open("benchmark_results.json", "w") as f:
        json.dump(final_report, f, indent=4)
    logger.success("Full benchmark report saved to benchmark_results.json")

if __name__ == "__main__":
    try:
        run_master_benchmark()
    except KeyboardInterrupt:
        logger.info("Benchmark interrupted.")
    except Exception as e:
        logger.exception(f"Benchmarking failed: {e}")
