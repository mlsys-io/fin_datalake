import argparse
import csv
import json
import math
import statistics
import time
import tracemalloc
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import pandas as pd
import pyarrow as pa
import ray
import ray.serve as serve
from loguru import logger

from etl.runtime import ensure_ray, resolve_serve_response
from pipelines.benchmarks.benchmark_table_agent import BenchmarkTableAgent


DEFAULT_DATASET_SIZES = (10_000, 100_000, 500_000, 1_000_000)
DEFAULT_TRIALS = 30
DEFAULT_OUTPUT_ROOT = Path("benchmark-results/zero-copy")
DEFAULT_SYMBOL = "BTCUSD"
BENCHMARK_AGENT_NAME = "BenchmarkAgent-1"


def _timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _ensure_benchmark_agent() -> Any:
    ensure_ray()
    BenchmarkTableAgent.deploy(name=BENCHMARK_AGENT_NAME)
    return serve.get_app_handle(BENCHMARK_AGENT_NAME)


def _build_market_dataframe(size: int) -> pd.DataFrame:
    base_timestamp = 1_710_000_000.0
    rows = list(range(size))
    opens = [60000.0 + (idx % 500) * 0.1 for idx in rows]
    closes = [value + (((idx % 7) - 3) * 0.15) for idx, value in enumerate(opens)]
    highs = [max(open_v, close_v) + 0.2 for open_v, close_v in zip(opens, closes)]
    lows = [min(open_v, close_v) - 0.2 for open_v, close_v in zip(opens, closes)]
    volumes = [1.0 + ((idx % 20) * 0.05) for idx in rows]
    timestamps = [base_timestamp + idx for idx in rows]
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
        }
    )


def _empty_market_state() -> Dict[str, Any]:
    return {"benchmark_mode": "zero_copy"}


def _run_serialized_trial(handle: Any, df: pd.DataFrame) -> Dict[str, float]:
    tracemalloc.start()
    started = time.perf_counter()
    response = handle.ask.remote(
        {
            "symbol": DEFAULT_SYMBOL,
            "ohlc_data": df.to_dict(orient="records"),
            "market_state": _empty_market_state(),
            "headlines": [],
        }
    )
    _ = resolve_serve_response(response)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {
        "elapsed_ms": elapsed_ms,
        "peak_mem_mb": peak_bytes / (1024 * 1024),
    }


def _run_zero_copy_trial(handle: Any, df: pd.DataFrame) -> Dict[str, float]:
    table = pa.Table.from_pandas(df, preserve_index=False)
    tracemalloc.start()
    started = time.perf_counter()
    data_ref = ray.put(table)
    response = handle.ask.remote(
        {
            "symbol": DEFAULT_SYMBOL,
            "data_ref": data_ref,
            "market_state": _empty_market_state(),
            "headlines": [],
        }
    )
    _ = resolve_serve_response(response)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {
        "elapsed_ms": elapsed_ms,
        "peak_mem_mb": peak_bytes / (1024 * 1024),
    }


def _build_trial_rows(
    *,
    size: int,
    trials: int,
    handle: Any,
) -> List[Dict[str, Any]]:
    logger.info(f"[ZeroCopy] Running {trials} trials for dataset size {size:,}")
    df = _build_market_dataframe(size)
    rows: List[Dict[str, Any]] = []

    for trial_idx in range(1, trials + 1):
        serialized = _run_serialized_trial(handle, df)
        zero_copy = _run_zero_copy_trial(handle, df)

        rows.append(
            {
                "dataset_size": size,
                "trial": trial_idx,
                "path": "serialized",
                "elapsed_ms": serialized["elapsed_ms"],
                "peak_mem_mb": serialized["peak_mem_mb"],
            }
        )
        rows.append(
            {
                "dataset_size": size,
                "trial": trial_idx,
                "path": "zero_copy",
                "elapsed_ms": zero_copy["elapsed_ms"],
                "peak_mem_mb": zero_copy["peak_mem_mb"],
            }
        )

        logger.info(
            "[ZeroCopy] trial={} size={} serialized={:.2f}ms zero_copy={:.2f}ms",
            trial_idx,
            f"{size:,}",
            serialized["elapsed_ms"],
            zero_copy["elapsed_ms"],
        )

    return rows


def _group_rows(rows: Iterable[Dict[str, Any]], *, size: int, path: str) -> List[Dict[str, Any]]:
    return [row for row in rows if row["dataset_size"] == size and row["path"] == path]


def _aggregate(rows: List[Dict[str, Any]], dataset_sizes: Sequence[int]) -> List[Dict[str, Any]]:
    summary: List[Dict[str, Any]] = []
    for size in dataset_sizes:
        serialized_rows = _group_rows(rows, size=size, path="serialized")
        zero_copy_rows = _group_rows(rows, size=size, path="zero_copy")

        serialized_latencies = [row["elapsed_ms"] for row in serialized_rows]
        zero_copy_latencies = [row["elapsed_ms"] for row in zero_copy_rows]
        serialized_mem = [row["peak_mem_mb"] for row in serialized_rows]
        zero_copy_mem = [row["peak_mem_mb"] for row in zero_copy_rows]

        serialized_mean = statistics.mean(serialized_latencies)
        zero_copy_mean = statistics.mean(zero_copy_latencies)
        summary.append(
            {
                "dataset_size": size,
                "serialized": {
                    "mean_ms": serialized_mean,
                    "std_ms": statistics.stdev(serialized_latencies) if len(serialized_latencies) > 1 else 0.0,
                    "median_ms": statistics.median(serialized_latencies),
                    "mean_peak_mem_mb": statistics.mean(serialized_mem),
                },
                "zero_copy": {
                    "mean_ms": zero_copy_mean,
                    "std_ms": statistics.stdev(zero_copy_latencies) if len(zero_copy_latencies) > 1 else 0.0,
                    "median_ms": statistics.median(zero_copy_latencies),
                    "mean_peak_mem_mb": statistics.mean(zero_copy_mem),
                },
                "speedup": serialized_mean / zero_copy_mean if zero_copy_mean else math.inf,
            }
        )
    return summary


def _write_trials_csv(rows: List[Dict[str, Any]], output_dir: Path) -> Path:
    path = output_dir / "zero_copy_trials.csv"
    fieldnames = ["dataset_size", "trial", "path", "elapsed_ms", "peak_mem_mb"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _write_summary_json(summary: List[Dict[str, Any]], output_dir: Path, *, trials: int) -> Path:
    path = output_dir / "zero_copy_summary.json"
    payload = {
        "benchmark": "zero_copy_handoff",
        "trials_per_size": trials,
        "dataset_sizes": [entry["dataset_size"] for entry in summary],
        "results": summary,
        "headline_speedup": summary[-1]["speedup"] if summary else None,
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def _build_svg_chart(summary: List[Dict[str, Any]], output_dir: Path) -> Path:
    path = output_dir / "zero_copy_latency.svg"
    width = 900
    height = 520
    left = 90
    right = 40
    top = 40
    bottom = 80
    plot_width = width - left - right
    plot_height = height - top - bottom

    xs = [entry["dataset_size"] for entry in summary]
    serialized_ys = [entry["serialized"]["mean_ms"] for entry in summary]
    zero_copy_ys = [entry["zero_copy"]["mean_ms"] for entry in summary]
    max_y = max(serialized_ys + zero_copy_ys) if summary else 1.0
    max_y *= 1.1

    def x_scale(value: int) -> float:
        if len(xs) == 1:
            return left + plot_width / 2
        index = xs.index(value)
        return left + (plot_width * index / (len(xs) - 1))

    def y_scale(value: float) -> float:
        return top + plot_height - ((value / max_y) * plot_height if max_y else 0.0)

    def polyline(points: List[tuple[float, float]], color: str) -> str:
        coords = " ".join(f"{x:.2f},{y:.2f}" for x, y in points)
        return (
            f'<polyline fill="none" stroke="{color}" stroke-width="3" '
            f'stroke-linejoin="round" stroke-linecap="round" points="{coords}" />'
        )

    serialized_points = [(x_scale(entry["dataset_size"]), y_scale(entry["serialized"]["mean_ms"])) for entry in summary]
    zero_points = [(x_scale(entry["dataset_size"]), y_scale(entry["zero_copy"]["mean_ms"])) for entry in summary]

    y_ticks = 5
    grid_lines = []
    labels = []
    for idx in range(y_ticks + 1):
        value = max_y * idx / y_ticks
        y = y_scale(value)
        grid_lines.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_width}" y2="{y:.2f}" stroke="#e5e7eb" />')
        labels.append(
            f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" font-size="12" fill="#374151">{value:.1f}</text>'
        )

    x_labels = []
    for value in xs:
        x = x_scale(value)
        x_labels.append(
            f'<text x="{x:.2f}" y="{height - bottom + 25}" text-anchor="middle" font-size="12" fill="#374151">{value:,}</text>'
        )

    circles = []
    for points, color in ((serialized_points, "#dc2626"), (zero_points, "#2563eb")):
        for x, y in points:
            circles.append(f'<circle cx="{x:.2f}" cy="{y:.2f}" r="4" fill="{color}" />')

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="white" />
<text x="{width/2:.1f}" y="24" text-anchor="middle" font-size="20" font-weight="600" fill="#111827">Zero-Copy vs JSON Latency</text>
<text x="{width/2:.1f}" y="{height - 20}" text-anchor="middle" font-size="14" fill="#374151">Dataset size (rows)</text>
<text x="24" y="{height/2:.1f}" transform="rotate(-90 24,{height/2:.1f})" text-anchor="middle" font-size="14" fill="#374151">Mean latency (ms)</text>
<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#111827" />
<line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#111827" />
{''.join(grid_lines)}
{''.join(labels)}
{''.join(x_labels)}
{polyline(serialized_points, "#dc2626")}
{polyline(zero_points, "#2563eb")}
{''.join(circles)}
<rect x="{width - 230}" y="50" width="170" height="54" rx="8" fill="#f9fafb" stroke="#d1d5db" />
<line x1="{width - 212}" y1="72" x2="{width - 180}" y2="72" stroke="#dc2626" stroke-width="3" />
<text x="{width - 168}" y="76" font-size="12" fill="#111827">Serialized</text>
<line x1="{width - 212}" y1="92" x2="{width - 180}" y2="92" stroke="#2563eb" stroke-width="3" />
<text x="{width - 168}" y="96" font-size="12" fill="#111827">Zero-copy</text>
</svg>"""
    path.write_text(svg, encoding="utf-8")
    return path


def _print_summary(summary: List[Dict[str, Any]]) -> None:
    print("\n" + "=" * 88)
    print(
        f"{'Rows':<12} | {'JSON mean+/-std (ms)':>24} | {'Zero-copy mean+/-std (ms)':>28} | {'Speedup':>10}"
    )
    print("-" * 88)
    for entry in summary:
        print(
            f"{entry['dataset_size']:<12,} | "
            f"{entry['serialized']['mean_ms']:>8.2f} +/- {entry['serialized']['std_ms']:<8.2f} | "
            f"{entry['zero_copy']['mean_ms']:>8.2f} +/- {entry['zero_copy']['std_ms']:<8.2f} | "
            f"{entry['speedup']:>8.2f}x"
        )
    print("=" * 88 + "\n")


def run_zero_copy_benchmark(
    *,
    trials: int = DEFAULT_TRIALS,
    dataset_sizes: Sequence[int] = DEFAULT_DATASET_SIZES,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> Dict[str, Any]:
    """
    Report-grade zero-copy experiment for market-table handoff latency.
    """
    logger.info("=== ZERO-COPY VS JSON SERIALIZATION BENCHMARK ===")
    logger.info("This benchmark measures end-to-end handoff latency from driver to agent-accessible data.")

    handle = _ensure_benchmark_agent()
    output_dir = Path(output_root) / _timestamp_slug()
    output_dir.mkdir(parents=True, exist_ok=True)

    trial_rows: List[Dict[str, Any]] = []
    for size in dataset_sizes:
        trial_rows.extend(_build_trial_rows(size=int(size), trials=trials, handle=handle))

    summary = _aggregate(trial_rows, dataset_sizes)
    csv_path = _write_trials_csv(trial_rows, output_dir)
    json_path = _write_summary_json(summary, output_dir, trials=trials)
    chart_path = _build_svg_chart(summary, output_dir)

    _print_summary(summary)
    logger.success(f"Saved raw trials to {csv_path}")
    logger.success(f"Saved summary JSON to {json_path}")
    logger.success(f"Saved chart to {chart_path}")

    return {
        "raw_trials": trial_rows,
        "summary": summary,
        "artifacts": {
            "output_dir": str(output_dir),
            "trials_csv": str(csv_path),
            "summary_json": str(json_path),
            "chart_svg": str(chart_path),
        },
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the zero-copy market-table benchmark.")
    parser.add_argument("--trials", type=int, default=DEFAULT_TRIALS, help="Number of trials per dataset size.")
    parser.add_argument(
        "--sizes",
        type=int,
        nargs="*",
        default=list(DEFAULT_DATASET_SIZES),
        help="Dataset sizes to benchmark.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_ROOT),
        help="Root directory for benchmark artifacts.",
    )
    parser.add_argument("--json", action="store_true", help="Print the final benchmark payload as JSON.")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        args = _parse_args()
        result = run_zero_copy_benchmark(
            trials=args.trials,
            dataset_sizes=args.sizes,
            output_root=args.output_dir,
        )
        if args.json:
            print(json.dumps(result, indent=2))
    except KeyboardInterrupt:
        logger.info("Benchmark interrupted.")
