import argparse
import csv
import json
import statistics
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from loguru import logger

from pipelines.benchmarks.baselines.baseline_plain import run_plain_baseline
from pipelines.benchmarks.baselines.baseline_spark_glue import run_spark_glue_baseline
from pipelines.benchmarks.contracts import SYSTEM_MARKET_PULSE, SYSTEM_PLAIN, SYSTEM_SPARK_GLUE
from pipelines.benchmarks.shared import capture_trial_input
from pipelines.market_pulse_demo import DEFAULT_PROVIDER, DEFAULT_SYMBOL, run_market_pulse_flow


DEFAULT_TRIALS = 30
DEFAULT_OUTPUT_ROOT = Path("benchmark-results/market-pulse")
SYSTEM_ORDER = [SYSTEM_PLAIN, SYSTEM_SPARK_GLUE, SYSTEM_MARKET_PULSE]
STAGE_FIELD_MAP = {
    "total": "total_duration_seconds",
    "ingest": "ingest_duration_seconds",
    "agent_setup": "agent_setup_duration_seconds",
    "signal": "signal_duration_seconds",
    "persistence": "persistence_duration_seconds",
    "visibility": "visibility_duration_seconds",
}
TRIAL_FIELDNAMES = [
    "trial",
    "system_name",
    "success",
    "error",
    "provider_requested",
    "symbol",
    "shared_input_capture_duration_seconds",
    "total_duration_seconds",
    "ingest_duration_seconds",
    "agent_setup_duration_seconds",
    "signal_duration_seconds",
    "persistence_duration_seconds",
    "visibility_duration_seconds",
    "news_mode",
    "price_mode",
    "news_fallback_used",
    "price_fallback_used",
    "mode_classification",
]
SUMMARY_FIELDNAMES = [
    "system_name",
    "scope",
    "stage",
    "trial_count",
    "mean_seconds",
    "std_seconds",
    "median_seconds",
    "min_seconds",
    "max_seconds",
    "p95_seconds",
]
MODE_BUCKETS = ("fully_live", "partial_fallback", "fully_fallback")
SYSTEM_LABELS = {
    SYSTEM_PLAIN: "Plain",
    SYSTEM_SPARK_GLUE: "Spark + Glue",
    SYSTEM_MARKET_PULSE: "Integrated",
}
SYSTEM_COLORS = {
    SYSTEM_PLAIN: "#1d4ed8",
    SYSTEM_SPARK_GLUE: "#d97706",
    SYSTEM_MARKET_PULSE: "#059669",
}


def _timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _mean(values: List[float]) -> float:
    return statistics.mean(values) if values else 0.0


def _std(values: List[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def _median(values: List[float]) -> float:
    return statistics.median(values) if values else 0.0


def _p95(values: List[float]) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
    return ordered[index]


def _mode_classification(news_meta: Dict[str, Any], ohlc_meta: Dict[str, Any]) -> str:
    news_fallback = bool(news_meta.get("fallback_used"))
    price_fallback = bool(ohlc_meta.get("fallback_used"))
    if news_fallback and price_fallback:
        return "fully_fallback"
    if news_fallback or price_fallback:
        return "partial_fallback"
    return "fully_live"


def _empty_stage_summary() -> Dict[str, float]:
    return {
        "mean_seconds": 0.0,
        "std_seconds": 0.0,
        "median_seconds": 0.0,
        "min_seconds": 0.0,
        "max_seconds": 0.0,
        "p95_seconds": 0.0,
    }


def _stage_summary(rows: List[Dict[str, Any]], key: str) -> Dict[str, float]:
    values = [float(row[key]) for row in rows]
    return {
        "mean_seconds": _mean(values),
        "std_seconds": _std(values),
        "median_seconds": _median(values),
        "min_seconds": min(values) if values else 0.0,
        "max_seconds": max(values) if values else 0.0,
        "p95_seconds": _p95(values),
    }


def _successful_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [row for row in rows if row.get("success")]


def _rows_for_system(rows: List[Dict[str, Any]], system_name: str) -> List[Dict[str, Any]]:
    return [row for row in rows if row.get("system_name") == system_name]


def _rows_for_mode(rows: List[Dict[str, Any]], mode: str) -> List[Dict[str, Any]]:
    return [row for row in rows if row.get("mode_classification") == mode]


def _stage_summaries(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    if not rows:
        return {stage: _empty_stage_summary() for stage in STAGE_FIELD_MAP}
    return {stage: _stage_summary(rows, key) for stage, key in STAGE_FIELD_MAP.items()}


def _system_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    successful_rows = _successful_rows(rows)
    mode_counts = {mode: 0 for mode in MODE_BUCKETS}
    for row in successful_rows:
        mode = str(row.get("mode_classification"))
        if mode in mode_counts:
            mode_counts[mode] += 1

    per_mode: Dict[str, Any] = {}
    for mode in MODE_BUCKETS:
        mode_rows = _rows_for_mode(successful_rows, mode)
        per_mode[mode] = {
            "trial_count": len(mode_rows),
            "stages": _stage_summaries(mode_rows),
        }

    return {
        "trial_count": len(rows),
        "success_count": len(successful_rows),
        "failure_count": len(rows) - len(successful_rows),
        "mode_counts": mode_counts,
        "stages": _stage_summaries(successful_rows),
        "per_mode": per_mode,
    }


def _build_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    systems = {system_name: _system_summary(_rows_for_system(rows, system_name)) for system_name in SYSTEM_ORDER}

    def _mean_total(system_name: str) -> float:
        return float(systems[system_name]["stages"]["total"]["mean_seconds"])

    integrated_mean = _mean_total(SYSTEM_MARKET_PULSE)
    comparison = {
        "fastest_mean_system": min(
            SYSTEM_ORDER,
            key=lambda system_name: systems[system_name]["stages"]["total"]["mean_seconds"]
            if systems[system_name]["success_count"] > 0
            else float("inf"),
        ),
        "plain_over_integrated_ratio": (_mean_total(SYSTEM_PLAIN) / integrated_mean) if integrated_mean else 0.0,
        "spark_glue_over_integrated_ratio": (_mean_total(SYSTEM_SPARK_GLUE) / integrated_mean) if integrated_mean else 0.0,
    }

    return {
        "trial_count": len({int(row["trial"]) for row in rows}),
        "row_count": len(rows),
        "systems": systems,
        "comparison": comparison,
    }


def _row_from_integrated_result(trial: int, result: Dict[str, Any] | None = None, *, error: str | None = None) -> Dict[str, Any]:
    result = dict(result or {})
    news_meta = dict(result.get("news_meta") or {})
    ohlc_meta = dict(result.get("ohlc_meta") or {})
    success = error is None
    return {
        "trial": trial,
        "system_name": SYSTEM_MARKET_PULSE,
        "success": success,
        "error": error or "",
        "provider_requested": result.get("provider_requested"),
        "symbol": result.get("symbol"),
        "shared_input_capture_duration_seconds": 0.0,
        "total_duration_seconds": float(result.get("total_duration_seconds", result.get("duration_seconds", 0.0))),
        "ingest_duration_seconds": float(result.get("ingest_duration_seconds", 0.0)),
        "agent_setup_duration_seconds": float(result.get("agent_setup_duration_seconds", 0.0)),
        "signal_duration_seconds": float(result.get("signal_duration_seconds", 0.0)),
        "persistence_duration_seconds": float(result.get("persistence_duration_seconds", 0.0)),
        "visibility_duration_seconds": float(result.get("visibility_duration_seconds", 0.0)),
        "news_mode": str(news_meta.get("mode", "unknown")),
        "price_mode": str(ohlc_meta.get("mode", "unknown")),
        "news_fallback_used": bool(news_meta.get("fallback_used")),
        "price_fallback_used": bool(ohlc_meta.get("fallback_used")),
        "mode_classification": _mode_classification(news_meta, ohlc_meta) if success else "failed",
    }


def _row_from_baseline_result(
    trial: int,
    system_name: str,
    result: Dict[str, Any] | None = None,
    *,
    shared_capture_duration_seconds: float = 0.0,
    error: str | None = None,
) -> Dict[str, Any]:
    result = dict(result or {})
    news_meta = dict(result.get("news_meta") or {})
    ohlc_meta = dict(result.get("ohlc_meta") or {})
    timings = dict(result.get("timings") or {})
    success = error is None and bool(result.get("success", True))
    signal_and_persist_total = float(timings.get("total_duration_seconds", 0.0))
    return {
        "trial": trial,
        "system_name": system_name,
        "success": success,
        "error": error or str(result.get("error", "")),
        "provider_requested": result.get("provider_requested"),
        "symbol": result.get("symbol"),
        "shared_input_capture_duration_seconds": float(shared_capture_duration_seconds),
        "total_duration_seconds": float(shared_capture_duration_seconds) + signal_and_persist_total,
        "ingest_duration_seconds": float(shared_capture_duration_seconds),
        "agent_setup_duration_seconds": 0.0,
        "signal_duration_seconds": float(timings.get("signal_duration_seconds", 0.0)),
        "persistence_duration_seconds": float(timings.get("persistence_duration_seconds", 0.0)),
        "visibility_duration_seconds": 0.0,
        "news_mode": str(news_meta.get("mode", "unknown")),
        "price_mode": str(ohlc_meta.get("mode", "unknown")),
        "news_fallback_used": bool(news_meta.get("fallback_used")),
        "price_fallback_used": bool(ohlc_meta.get("fallback_used")),
        "mode_classification": _mode_classification(news_meta, ohlc_meta) if success else "failed",
    }


def _summary_rows(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for system_name in SYSTEM_ORDER:
        system_summary = dict(summary.get("systems", {}).get(system_name) or {})
        scopes = [("overall", system_summary.get("stages", {}))]
        scopes.extend((mode, system_summary.get("per_mode", {}).get(mode, {}).get("stages", {})) for mode in MODE_BUCKETS)
        for scope, stages in scopes:
            trial_count = system_summary.get("trial_count", 0) if scope == "overall" else system_summary.get("per_mode", {}).get(scope, {}).get("trial_count", 0)
            for stage, stats in stages.items():
                rows.append(
                    {
                        "system_name": system_name,
                        "scope": scope,
                        "stage": stage,
                        "trial_count": trial_count,
                        "mean_seconds": float(stats.get("mean_seconds", 0.0)),
                        "std_seconds": float(stats.get("std_seconds", 0.0)),
                        "median_seconds": float(stats.get("median_seconds", 0.0)),
                        "min_seconds": float(stats.get("min_seconds", 0.0)),
                        "max_seconds": float(stats.get("max_seconds", 0.0)),
                        "p95_seconds": float(stats.get("p95_seconds", 0.0)),
                    }
                )
    return rows


def _write_trials_csv(rows: List[Dict[str, Any]], output_dir: Path) -> Path:
    path = output_dir / "market_pulse_trials.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=TRIAL_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    return path


def _write_summary_csv(summary: Dict[str, Any], output_dir: Path) -> Path:
    path = output_dir / "market_pulse_summary.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDNAMES)
        writer.writeheader()
        writer.writerows(_summary_rows(summary))
    return path


def _write_summary_json(summary: Dict[str, Any], rows: List[Dict[str, Any]], output_dir: Path) -> Path:
    path = output_dir / "market_pulse_summary.json"
    payload = {
        "benchmark": "market_pulse_end_to_end_comparative",
        "summary": summary,
        "trials": rows,
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def _build_system_comparison_svg(summary: Dict[str, Any], output_dir: Path) -> Path:
    path = output_dir / "market_pulse_system_comparison.svg"
    width = 980
    height = 560
    left = 90
    right = 40
    top = 50
    bottom = 100
    plot_width = width - left - right
    plot_height = height - top - bottom
    bar_width = 140

    systems = summary.get("systems", {})
    max_y = 1.0
    for system_name in SYSTEM_ORDER:
        max_y = max(max_y, float(systems.get(system_name, {}).get("stages", {}).get("total", {}).get("mean_seconds", 0.0)))
    max_y *= 1.15

    def y_scale(value: float) -> float:
        return top + plot_height - ((value / max_y) * plot_height if max_y else 0.0)

    x_positions = {
        system_name: left + ((idx + 0.5) * plot_width / len(SYSTEM_ORDER))
        for idx, system_name in enumerate(SYSTEM_ORDER)
    }

    grid_lines = []
    y_labels = []
    for idx in range(6):
        value = max_y * idx / 5
        y = y_scale(value)
        grid_lines.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_width}" y2="{y:.2f}" stroke="#e5e7eb" />')
        y_labels.append(f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" font-size="12" fill="#374151">{value:.2f}</text>')

    bars = []
    labels = []
    for system_name in SYSTEM_ORDER:
        system_summary = systems.get(system_name, {})
        total_summary = system_summary.get("stages", {}).get("total", {})
        mean_value = float(total_summary.get("mean_seconds", 0.0))
        p95_value = float(total_summary.get("p95_seconds", 0.0))
        x_center = x_positions[system_name]
        bar_top = y_scale(mean_value)
        color = SYSTEM_COLORS[system_name]
        bars.append(
            f'<rect x="{x_center - bar_width/2:.2f}" y="{bar_top:.2f}" width="{bar_width}" '
            f'height="{top + plot_height - bar_top:.2f}" fill="{color}" rx="6" />'
        )
        bars.append(
            f'<line x1="{x_center - bar_width/2:.2f}" y1="{y_scale(p95_value):.2f}" '
            f'x2="{x_center + bar_width/2:.2f}" y2="{y_scale(p95_value):.2f}" '
            f'stroke="#111827" stroke-width="3" stroke-dasharray="6 4" />'
        )
        labels.append(
            f'<text x="{x_center:.2f}" y="{height - bottom + 22}" text-anchor="middle" font-size="12" fill="#374151">{SYSTEM_LABELS[system_name]}</text>'
        )
        labels.append(
            f'<text x="{x_center:.2f}" y="{bar_top - 8:.2f}" text-anchor="middle" font-size="12" fill="#111827">{mean_value:.2f}s</text>'
        )

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="white" />
<text x="{width/2:.1f}" y="28" text-anchor="middle" font-size="20" font-weight="600" fill="#111827">Market Pulse System Comparison</text>
<text x="{width/2:.1f}" y="48" text-anchor="middle" font-size="13" fill="#374151">Bar = mean end-to-end latency, dashed line = P95 latency</text>
<text x="{width/2:.1f}" y="{height - 18}" text-anchor="middle" font-size="14" fill="#374151">System</text>
<text x="24" y="{height/2:.1f}" transform="rotate(-90 24,{height/2:.1f})" text-anchor="middle" font-size="14" fill="#374151">Latency (seconds)</text>
<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#111827" />
<line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#111827" />
{''.join(grid_lines)}
{''.join(y_labels)}
{''.join(bars)}
{''.join(labels)}
</svg>"""
    path.write_text(svg, encoding="utf-8")
    return path


def _build_integrated_stage_svg(summary: Dict[str, Any], output_dir: Path) -> Path:
    path = output_dir / "market_pulse_integrated_stage_breakdown.svg"
    width = 960
    height = 560
    left = 90
    right = 40
    top = 50
    bottom = 100
    plot_width = width - left - right
    plot_height = height - top - bottom
    bar_width = 90

    integrated_stages = [
        ("ingest", "#2563eb"),
        ("agent_setup", "#0f766e"),
        ("signal", "#dc2626"),
        ("persistence", "#7c3aed"),
        ("visibility", "#d97706"),
        ("total", "#111827"),
    ]
    integrated_summary = summary.get("systems", {}).get(SYSTEM_MARKET_PULSE, {}).get("stages", {})
    max_y = 1.0
    for stage, _ in integrated_stages:
        max_y = max(max_y, float(integrated_summary.get(stage, {}).get("mean_seconds", 0.0)))
    max_y *= 1.15

    def y_scale(value: float) -> float:
        return top + plot_height - ((value / max_y) * plot_height if max_y else 0.0)

    x_positions = {
        stage: left + ((idx + 0.5) * plot_width / len(integrated_stages))
        for idx, (stage, _) in enumerate(integrated_stages)
    }

    grid_lines = []
    y_labels = []
    for idx in range(6):
        value = max_y * idx / 5
        y = y_scale(value)
        grid_lines.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_width}" y2="{y:.2f}" stroke="#e5e7eb" />')
        y_labels.append(f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" font-size="12" fill="#374151">{value:.2f}</text>')

    bars = []
    labels = []
    for stage, color in integrated_stages:
        stage_summary = integrated_summary.get(stage, {})
        mean_value = float(stage_summary.get("mean_seconds", 0.0))
        p95_value = float(stage_summary.get("p95_seconds", 0.0))
        x_center = x_positions[stage]
        bar_top = y_scale(mean_value)
        bars.append(
            f'<rect x="{x_center - bar_width/2:.2f}" y="{bar_top:.2f}" width="{bar_width}" '
            f'height="{top + plot_height - bar_top:.2f}" fill="{color}" rx="6" />'
        )
        bars.append(
            f'<line x1="{x_center - bar_width/2:.2f}" y1="{y_scale(p95_value):.2f}" '
            f'x2="{x_center + bar_width/2:.2f}" y2="{y_scale(p95_value):.2f}" '
            f'stroke="#111827" stroke-width="3" stroke-dasharray="6 4" />'
        )
        labels.append(
            f'<text x="{x_center:.2f}" y="{height - bottom + 22}" text-anchor="middle" font-size="12" fill="#374151">{stage.replace("_", " ").title()}</text>'
        )
        labels.append(
            f'<text x="{x_center:.2f}" y="{bar_top - 8:.2f}" text-anchor="middle" font-size="12" fill="#111827">{mean_value:.2f}s</text>'
        )

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="white" />
<text x="{width/2:.1f}" y="28" text-anchor="middle" font-size="20" font-weight="600" fill="#111827">Integrated Workflow Stage Breakdown</text>
<text x="{width/2:.1f}" y="48" text-anchor="middle" font-size="13" fill="#374151">Bar = mean latency, dashed line = P95 latency</text>
<text x="{width/2:.1f}" y="{height - 18}" text-anchor="middle" font-size="14" fill="#374151">Workflow stage</text>
<text x="24" y="{height/2:.1f}" transform="rotate(-90 24,{height/2:.1f})" text-anchor="middle" font-size="14" fill="#374151">Latency (seconds)</text>
<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#111827" />
<line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#111827" />
{''.join(grid_lines)}
{''.join(y_labels)}
{''.join(bars)}
{''.join(labels)}
</svg>"""
    path.write_text(svg, encoding="utf-8")
    return path


def _print_summary(summary: Dict[str, Any]) -> None:
    print("\n" + "=" * 96)
    print("MARKET PULSE COMPARATIVE BENCHMARK")
    print("-" * 96)
    print(f"Trials per system   : {summary['trial_count']}")
    for system_name in SYSTEM_ORDER:
        system_summary = summary["systems"][system_name]
        total_summary = system_summary["stages"]["total"]
        print(
            f"{SYSTEM_LABELS[system_name]:<18} "
            f"success={system_summary['success_count']}/{system_summary['trial_count']} "
            f"mean={total_summary['mean_seconds']:.2f}s "
            f"std={total_summary['std_seconds']:.2f}s "
            f"p95={total_summary['p95_seconds']:.2f}s"
        )
    print(
        "Comparison ratios   : "
        f"plain/integrated={summary['comparison']['plain_over_integrated_ratio']:.2f}x, "
        f"spark/integrated={summary['comparison']['spark_glue_over_integrated_ratio']:.2f}x"
    )
    print("=" * 96 + "\n")


def run_master_benchmark(
    *,
    trials: int = DEFAULT_TRIALS,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    provider: str = DEFAULT_PROVIDER,
    symbol: str = DEFAULT_SYMBOL,
) -> Dict[str, Any]:
    """
    Executes repeated comparative trials for the Market Pulse workflow, the
    plain sequential baseline, and the Spark plus glue baseline.
    """
    logger.info("\n" + "#" * 60 + "\nMARKET PULSE COMPARATIVE BENCHMARK\n" + "#" * 60)
    logger.info("This benchmark compares plain, Spark+glue, and integrated Market Pulse execution.")

    output_dir = Path(output_root) / _timestamp_slug()
    output_dir.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, Any]] = []

    for trial in range(1, trials + 1):
        logger.info(f"[MarketPulseBenchmark] Starting comparative trial {trial}/{trials}")

        shared_input = None
        shared_capture_duration = 0.0
        shared_capture_error = None
        try:
            capture_timer_started = time.perf_counter()
            shared_input = capture_trial_input(provider=provider, symbol=symbol)
            shared_capture_duration = time.perf_counter() - capture_timer_started
            logger.info(
                "[MarketPulseBenchmark] Shared input captured in {:.2f}s for plain and Spark baselines",
                shared_capture_duration,
            )
        except Exception as exc:
            shared_capture_error = str(exc)
            logger.exception(f"[MarketPulseBenchmark] Shared input capture failed for trial {trial}: {exc}")

        if shared_input is not None:
            try:
                plain_result = run_plain_baseline(trial_input=shared_input, provider=provider, symbol=symbol)
                plain_row = _row_from_baseline_result(
                    trial,
                    SYSTEM_PLAIN,
                    plain_result,
                    shared_capture_duration_seconds=shared_capture_duration,
                )
            except Exception as exc:
                logger.exception(f"[MarketPulseBenchmark] Plain baseline trial {trial} failed: {exc}")
                plain_row = _row_from_baseline_result(
                    trial,
                    SYSTEM_PLAIN,
                    error=str(exc),
                    shared_capture_duration_seconds=shared_capture_duration,
                )
        else:
            plain_row = _row_from_baseline_result(
                trial,
                SYSTEM_PLAIN,
                error=f"shared_input_capture_failed: {shared_capture_error}",
                shared_capture_duration_seconds=shared_capture_duration,
            )
        rows.append(plain_row)

        if shared_input is not None:
            try:
                spark_result = run_spark_glue_baseline(trial_input=shared_input, provider=provider, symbol=symbol)
                spark_row = _row_from_baseline_result(
                    trial,
                    SYSTEM_SPARK_GLUE,
                    spark_result,
                    shared_capture_duration_seconds=shared_capture_duration,
                )
            except Exception as exc:
                logger.exception(f"[MarketPulseBenchmark] Spark baseline trial {trial} failed: {exc}")
                spark_row = _row_from_baseline_result(
                    trial,
                    SYSTEM_SPARK_GLUE,
                    error=str(exc),
                    shared_capture_duration_seconds=shared_capture_duration,
                )
        else:
            spark_row = _row_from_baseline_result(
                trial,
                SYSTEM_SPARK_GLUE,
                error=f"shared_input_capture_failed: {shared_capture_error}",
                shared_capture_duration_seconds=shared_capture_duration,
            )
        rows.append(spark_row)

        try:
            integrated_result = run_market_pulse_flow(provider=provider, symbol=symbol)
            integrated_row = _row_from_integrated_result(trial, integrated_result)
        except Exception as exc:
            logger.exception(f"[MarketPulseBenchmark] Integrated workflow trial {trial} failed: {exc}")
            integrated_row = _row_from_integrated_result(trial, error=str(exc))
        rows.append(integrated_row)

        for row in (plain_row, spark_row, integrated_row):
            if row["success"]:
                logger.info(
                    "[MarketPulseBenchmark] trial={} system={} total={:.2f}s ingest={:.2f}s signal={:.2f}s persistence={:.2f}s mode={}",
                    trial,
                    row["system_name"],
                    row["total_duration_seconds"],
                    row["ingest_duration_seconds"],
                    row["signal_duration_seconds"],
                    row["persistence_duration_seconds"],
                    row["mode_classification"],
                )
            else:
                logger.warning(
                    "[MarketPulseBenchmark] trial={} system={} failed error={}",
                    trial,
                    row["system_name"],
                    row["error"],
                )

    summary = _build_summary(rows)
    trials_csv_path = _write_trials_csv(rows, output_dir)
    summary_csv_path = _write_summary_csv(summary, output_dir)
    summary_json_path = _write_summary_json(summary, rows, output_dir)
    comparison_chart_path = _build_system_comparison_svg(summary, output_dir)
    integrated_stage_chart_path = _build_integrated_stage_svg(summary, output_dir)

    _print_summary(summary)
    logger.success(f"Saved raw trials to {trials_csv_path}")
    logger.success(f"Saved summary CSV to {summary_csv_path}")
    logger.success(f"Saved summary JSON to {summary_json_path}")
    logger.success(f"Saved system comparison chart to {comparison_chart_path}")
    logger.success(f"Saved integrated stage chart to {integrated_stage_chart_path}")

    return {
        "summary": summary,
        "trials": rows,
        "artifacts": {
            "output_dir": str(output_dir),
            "trials_csv": str(trials_csv_path),
            "summary_csv": str(summary_csv_path),
            "summary_json": str(summary_json_path),
            "comparison_chart_svg": str(comparison_chart_path),
            "integrated_stage_chart_svg": str(integrated_stage_chart_path),
        },
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Market Pulse comparative benchmark.")
    parser.add_argument("--trials", type=int, default=DEFAULT_TRIALS, help="Number of trials per system.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT), help="Root directory for benchmark artifacts.")
    parser.add_argument("--provider", default=DEFAULT_PROVIDER, help="Provider to request for benchmark runs.")
    parser.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Symbol to benchmark.")
    parser.add_argument("--json", action="store_true", help="Print the final benchmark payload as JSON.")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        args = _parse_args()
        result = run_master_benchmark(
            trials=args.trials,
            output_root=args.output_dir,
            provider=args.provider,
            symbol=args.symbol,
        )
        if args.json:
            print(json.dumps(result, indent=2))
    except KeyboardInterrupt:
        logger.info("Benchmark interrupted.")
    except Exception as exc:
        logger.exception(f"Benchmarking failed: {exc}")
