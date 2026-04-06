import argparse
import csv
import json
import random
import statistics
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4

import ray.serve as serve
from loguru import logger

from etl.agents.catalog import list_agent_catalog_entries, update_agent_catalog_control
from etl.agents.manager import delete_agent, deploy_agent
from etl.runtime import ensure_ray, resolve_serve_response


DEFAULT_TRIALS = 30
DEFAULT_OUTPUT_ROOT = Path("benchmark-results/mttr")
DEFAULT_AGENT_CLASS = "SupportAgent"
DEFAULT_AGENT_PREFIX = "SupportAgent-MTTR"
DEFAULT_POLL_INTERVAL_SECONDS = 1.0
DEFAULT_RECOVERY_TIMEOUT_SECONDS = 120.0
DEFAULT_DELETE_RETRIES = 5
DEFAULT_DELETE_RETRY_DELAY_SECONDS = 2.0
DEFAULT_INTER_TRIAL_DELAY_SECONDS = 10.0
DEFAULT_MANUAL_OPERATOR_DELAY_MODE = "uniform"
DEFAULT_MANUAL_OPERATOR_DELAY_SECONDS = 10.0
DEFAULT_MANUAL_OPERATOR_DELAY_MIN_SECONDS = 5.0
DEFAULT_MANUAL_OPERATOR_DELAY_MAX_SECONDS = 15.0
DEFAULT_MODE = "compare"
VALID_MODES = {"compare", "overseer", "manual"}
VALID_OPERATOR_DELAY_MODES = {"fixed", "uniform"}


def _timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _victim_name(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:8]}"


def _get_catalog_entry(name: str) -> Dict[str, Any] | None:
    for entry in list_agent_catalog_entries(runtime_source="ray-serve", enabled_only=False):
        if str(entry.get("name") or "").strip() == name:
            return entry
    return None


def _cleanup_victim(name: str) -> None:
    result = delete_agent(name, clean_catalog=True)
    if result["serve_deleted"]:
        logger.info(f"[cleanup] Removed Serve app '{name}'.")
    if result["catalog_deleted"]:
        logger.info(f"[cleanup] Removed durable catalog entry '{name}'.")


def _wait_for_probe_ready(name: str, timeout_seconds: float, message: str) -> str:
    deadline = time.perf_counter() + timeout_seconds
    last_error = None
    while time.perf_counter() < deadline:
        try:
            handle = serve.get_app_handle(name)
            response = resolve_serve_response(handle.chat.remote(message))
            return str(response)
        except Exception as exc:
            last_error = exc
            time.sleep(1.0)
    raise RuntimeError(f"Deployment '{name}' never became probe-ready: {last_error}")


def _set_overseer_management(name: str, managed: bool, *, notes: str) -> None:
    updated = update_agent_catalog_control(
        name=name,
        managed_by_overseer=managed,
        reconcile_notes=notes,
    )
    if not updated:
        raise RuntimeError(
            f"Failed to update catalog control flags for '{name}'. "
            "Manual MTTR trials require toggling managed_by_overseer."
        )


def _capture_state(name: str) -> Dict[str, str]:
    entry = _get_catalog_entry(name)
    return {
        "observed_status": str(entry.get("observed_status") or "missing") if entry else "missing",
        "health_status": str(entry.get("health_status") or "unknown") if entry else "unknown",
        "recovery_state": str(entry.get("recovery_state") or "unknown") if entry else "unknown",
    }


def _delete_for_outage(name: str) -> float:
    last_error = None
    for attempt in range(1, DEFAULT_DELETE_RETRIES + 1):
        delete_result = delete_agent(name, clean_catalog=False)
        if delete_result["serve_deleted"]:
            return time.perf_counter()
        last_error = (
            f"Serve deletion not confirmed on attempt {attempt}/{DEFAULT_DELETE_RETRIES} "
            f"for '{name}'."
        )
        logger.warning(f"[MTTR] {last_error}")
        if attempt < DEFAULT_DELETE_RETRIES:
            time.sleep(DEFAULT_DELETE_RETRY_DELAY_SECONDS)
    raise RuntimeError(
        f"Failed to delete Serve app '{name}' for MTTR trial after "
        f"{DEFAULT_DELETE_RETRIES} attempts. Last error: {last_error}"
    )


def _wait_for_overseer_recovery(
    *,
    victim_name: str,
    poll_interval_seconds: float,
    timeout_seconds: float,
    start_time: float,
) -> Dict[str, Any]:
    deadline = start_time + timeout_seconds
    observed_states: List[str] = []
    saw_outage = False

    while time.perf_counter() < deadline:
        time.sleep(poll_interval_seconds)
        state = _capture_state(victim_name)
        observed_status = state["observed_status"]
        observed_states.append(observed_status)

        try:
            handle = serve.get_app_handle(victim_name)
            probe_response = resolve_serve_response(handle.chat.remote("mttr probe"))
            if not saw_outage:
                logger.debug(
                    f"[MTTR] Catalog is {observed_status} for '{victim_name}', "
                    "but outage has not yet been confirmed by probe failure."
                )
                continue

            return {
                "success": True,
                "mttr_seconds": time.perf_counter() - start_time,
                "observed_states": observed_states,
                "final_observed_status": state["observed_status"],
                "final_health_status": state["health_status"],
                "final_recovery_state": state["recovery_state"],
                "probe_response": str(probe_response),
                "error": None,
            }
        except Exception as exc:
            saw_outage = True
            logger.debug(f"[MTTR] Outage confirmed for '{victim_name}': {exc}")

    state = _capture_state(victim_name)
    raise RuntimeError(
        "Recovery timed out. "
        f"Last observed={state['observed_status']}, "
        f"health={state['health_status']}, "
        f"recovery={state['recovery_state']}."
    )


def _wait_for_manual_outage_detection(
    *,
    victim_name: str,
    poll_interval_seconds: float,
    timeout_seconds: float,
) -> List[str]:
    deadline = time.perf_counter() + timeout_seconds
    observed_states: List[str] = []

    while time.perf_counter() < deadline:
        time.sleep(poll_interval_seconds)
        state = _capture_state(victim_name)
        observed_states.append(state["observed_status"])

        try:
            handle = serve.get_app_handle(victim_name)
            resolve_serve_response(handle.chat.remote("manual outage detection probe"))
        except Exception:
            return observed_states

    raise RuntimeError(f"Manual baseline never confirmed outage for '{victim_name}'.")


def _run_manual_recovery(
    *,
    victim_name: str,
    agent_class: str,
    poll_interval_seconds: float,
    timeout_seconds: float,
    operator_delay_seconds: float,
    start_time: float,
) -> Dict[str, Any]:
    logger.info(f"[manual] checking deployment status for '{victim_name}'")
    observed_states = _wait_for_manual_outage_detection(
        victim_name=victim_name,
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
    )

    logger.info(f"[manual] confirmed outage for '{victim_name}'")
    if operator_delay_seconds > 0:
        logger.info(
            f"[manual] waiting {operator_delay_seconds:.1f}s to simulate operator reaction time"
        )
        time.sleep(operator_delay_seconds)
    logger.info(f"[manual] redeploying '{victim_name}' via deploy_agent(...)")
    deploy_agent(agent_class, name=victim_name)
    logger.info(f"[manual] waiting for '{victim_name}' to become ready again")
    probe_response = _wait_for_probe_ready(victim_name, timeout_seconds=timeout_seconds, message="manual mttr probe")
    state = _capture_state(victim_name)

    return {
        "success": True,
        "mttr_seconds": time.perf_counter() - start_time,
        "observed_states": observed_states,
        "final_observed_status": state["observed_status"],
        "final_health_status": state["health_status"],
        "final_recovery_state": state["recovery_state"],
        "probe_response": probe_response,
        "operator_delay_seconds": operator_delay_seconds,
        "error": None,
    }


def _sample_manual_operator_delay(
    *,
    rng: random.Random,
    mode: str,
    fixed_seconds: float,
    min_seconds: float,
    max_seconds: float,
) -> float:
    if mode == "fixed":
        return max(0.0, fixed_seconds)
    if mode == "uniform":
        lower = max(0.0, min(min_seconds, max_seconds))
        upper = max(lower, max(min_seconds, max_seconds))
        return rng.uniform(lower, upper)
    raise ValueError(f"Unsupported manual operator delay mode: {mode}")


def _run_mttr_trial(
    *,
    mode: str,
    agent_class: str,
    victim_prefix: str,
    poll_interval_seconds: float,
    timeout_seconds: float,
    manual_operator_delay_seconds: float,
    manual_operator_delay_mode: str,
    manual_operator_delay_min_seconds: float,
    manual_operator_delay_max_seconds: float,
    rng: random.Random,
) -> Dict[str, Any]:
    ensure_ray()
    victim_name = _victim_name(victim_prefix)

    try:
        logger.info(f"[MTTR/{mode}] Deploying trial victim '{victim_name}'")
        deploy_agent(agent_class, name=victim_name)
        _wait_for_probe_ready(victim_name, timeout_seconds=30.0, message="warmup")

        if mode == "manual":
            logger.info(f"[manual] disabling Overseer management for '{victim_name}'")
            _set_overseer_management(
                victim_name,
                False,
                notes="Manual MTTR benchmark temporarily disabled Overseer management.",
            )

        logger.warning(f"[MTTR/{mode}] Deleting Serve app '{victim_name}' to simulate outage")
        start_time = _delete_for_outage(victim_name)

        if mode == "overseer":
            result = _wait_for_overseer_recovery(
                victim_name=victim_name,
                poll_interval_seconds=poll_interval_seconds,
                timeout_seconds=timeout_seconds,
                start_time=start_time,
            )
        elif mode == "manual":
            operator_delay_seconds = _sample_manual_operator_delay(
                rng=rng,
                mode=manual_operator_delay_mode,
                fixed_seconds=manual_operator_delay_seconds,
                min_seconds=manual_operator_delay_min_seconds,
                max_seconds=manual_operator_delay_max_seconds,
            )
            result = _run_manual_recovery(
                victim_name=victim_name,
                agent_class=agent_class,
                poll_interval_seconds=poll_interval_seconds,
                timeout_seconds=timeout_seconds,
                operator_delay_seconds=operator_delay_seconds,
                start_time=start_time,
            )
        else:
            raise ValueError(f"Unsupported benchmark mode: {mode}")

        result.update(
            {
                "mode": mode,
                "victim_name": victim_name,
                "agent_class": agent_class,
            }
        )
        return result
    finally:
        _cleanup_victim(victim_name)


def _mean(values: List[float]) -> float:
    return statistics.mean(values) if values else 0.0


def _std(values: List[float]) -> float:
    return statistics.stdev(values) if len(values) > 1 else 0.0


def _p95(values: List[float]) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
    return ordered[index]


def _build_mode_summary(trials: List[Dict[str, Any]]) -> Dict[str, Any]:
    successful = [trial for trial in trials if trial["success"] and trial["mttr_seconds"] is not None]
    mttrs = [float(trial["mttr_seconds"]) for trial in successful]
    return {
        "trial_count": len(trials),
        "success_count": len(successful),
        "failure_count": len(trials) - len(successful),
        "mean_mttr_seconds": _mean(mttrs),
        "std_mttr_seconds": _std(mttrs),
        "min_mttr_seconds": min(mttrs) if mttrs else None,
        "max_mttr_seconds": max(mttrs) if mttrs else None,
        "p95_mttr_seconds": _p95(mttrs) if mttrs else None,
    }


def _build_summary(trials_by_mode: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    mode_summaries = {mode: _build_mode_summary(mode_trials) for mode, mode_trials in trials_by_mode.items()}
    comparison: Dict[str, Any] = {}
    overseer_mean = mode_summaries.get("overseer", {}).get("mean_mttr_seconds")
    manual_mean = mode_summaries.get("manual", {}).get("mean_mttr_seconds")
    if overseer_mean and manual_mean:
        comparison["manual_over_overseer_ratio"] = manual_mean / overseer_mean
        comparison["overseer_improvement_pct"] = ((manual_mean - overseer_mean) / manual_mean) * 100.0
    return {
        "modes": mode_summaries,
        "comparison": comparison,
    }


def _write_trials_csv(trials_by_mode: Dict[str, List[Dict[str, Any]]], output_dir: Path) -> Path:
    path = output_dir / "mttr_trials.csv"
    fieldnames = [
        "trial",
        "mode",
        "success",
        "agent_class",
        "victim_name",
        "mttr_seconds",
        "operator_delay_seconds",
        "final_observed_status",
        "final_health_status",
        "final_recovery_state",
        "error",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for mode, mode_trials in trials_by_mode.items():
            for idx, trial in enumerate(mode_trials, start=1):
                writer.writerow(
                    {
                        "trial": idx,
                        "mode": mode,
                        "success": trial["success"],
                        "agent_class": trial["agent_class"],
                        "victim_name": trial["victim_name"],
                        "mttr_seconds": trial["mttr_seconds"],
                        "operator_delay_seconds": trial.get("operator_delay_seconds"),
                        "final_observed_status": trial["final_observed_status"],
                        "final_health_status": trial["final_health_status"],
                        "final_recovery_state": trial["final_recovery_state"],
                        "error": trial["error"],
                    }
                )
    return path


def _write_summary_json(
    summary: Dict[str, Any],
    trials_by_mode: Dict[str, List[Dict[str, Any]]],
    output_dir: Path,
    *,
    selected_mode: str,
) -> Path:
    path = output_dir / "mttr_summary.json"
    payload = {
        "benchmark": "mttr_recovery_comparison",
        "selected_mode": selected_mode,
        "summary": summary,
        "trials": trials_by_mode,
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path


def _build_svg_chart(summary: Dict[str, Any], output_dir: Path) -> Path:
    path = output_dir / "mttr_comparison.svg"
    width = 900
    height = 520
    left = 90
    right = 40
    top = 40
    bottom = 90
    plot_width = width - left - right
    plot_height = height - top - bottom
    bar_width = 150
    modes = [mode for mode in ("overseer", "manual") if mode in summary["modes"]]
    max_y = 1.0
    for mode in modes:
        mode_summary = summary["modes"][mode]
        for key in ("mean_mttr_seconds", "p95_mttr_seconds", "max_mttr_seconds"):
            value = mode_summary.get(key)
            if value:
                max_y = max(max_y, float(value))
    max_y *= 1.1

    def y_scale(value: float) -> float:
        return top + plot_height - ((value / max_y) * plot_height if max_y else 0.0)

    x_positions = {
        mode: left + ((idx + 0.5) * plot_width / max(len(modes), 1))
        for idx, mode in enumerate(modes)
    }

    grid_lines = []
    y_labels = []
    for idx in range(6):
        value = max_y * idx / 5
        y = y_scale(value)
        grid_lines.append(f'<line x1="{left}" y1="{y:.2f}" x2="{left + plot_width}" y2="{y:.2f}" stroke="#e5e7eb" />')
        y_labels.append(
            f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" font-size="12" fill="#374151">{value:.1f}</text>'
        )

    bars = []
    extra_lines = []
    labels = []
    colors = {"overseer": "#2563eb", "manual": "#dc2626"}
    for mode in modes:
        mode_summary = summary["modes"][mode]
        x_center = x_positions[mode]
        mean_value = float(mode_summary.get("mean_mttr_seconds") or 0.0)
        p95_value = float(mode_summary.get("p95_mttr_seconds") or 0.0)
        bar_top = y_scale(mean_value)
        bars.append(
            f'<rect x="{x_center - bar_width/2:.2f}" y="{bar_top:.2f}" width="{bar_width}" '
            f'height="{top + plot_height - bar_top:.2f}" fill="{colors[mode]}" rx="6" />'
        )
        extra_lines.append(
            f'<line x1="{x_center - bar_width/2:.2f}" y1="{y_scale(p95_value):.2f}" '
            f'x2="{x_center + bar_width/2:.2f}" y2="{y_scale(p95_value):.2f}" '
            f'stroke="#111827" stroke-width="3" stroke-dasharray="6 4" />'
        )
        labels.append(
            f'<text x="{x_center:.2f}" y="{height - bottom + 25}" text-anchor="middle" font-size="13" fill="#374151">{mode.title()}</text>'
        )
        labels.append(
            f'<text x="{x_center:.2f}" y="{bar_top - 8:.2f}" text-anchor="middle" font-size="12" fill="#111827">{mean_value:.1f}s</text>'
        )

    ratio_text = ""
    ratio = summary.get("comparison", {}).get("manual_over_overseer_ratio")
    if ratio:
        ratio_text = f'<text x="{width/2:.1f}" y="46" text-anchor="middle" font-size="13" fill="#374151">Manual / Overseer mean MTTR: {ratio:.2f}x</text>'

    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="white" />
<text x="{width/2:.1f}" y="24" text-anchor="middle" font-size="20" font-weight="600" fill="#111827">Comparative MTTR Benchmark</text>
{ratio_text}
<text x="{width/2:.1f}" y="{height - 20}" text-anchor="middle" font-size="14" fill="#374151">Recovery mode</text>
<text x="24" y="{height/2:.1f}" transform="rotate(-90 24,{height/2:.1f})" text-anchor="middle" font-size="14" fill="#374151">MTTR (seconds)</text>
<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_height}" stroke="#111827" />
<line x1="{left}" y1="{top + plot_height}" x2="{left + plot_width}" y2="{top + plot_height}" stroke="#111827" />
{''.join(grid_lines)}
{''.join(y_labels)}
{''.join(bars)}
{''.join(extra_lines)}
{''.join(labels)}
<rect x="{width - 270}" y="72" width="210" height="74" rx="8" fill="#f9fafb" stroke="#d1d5db" />
<rect x="{width - 250}" y="92" width="20" height="14" fill="#2563eb" rx="3" />
<text x="{width - 220}" y="103" font-size="12" fill="#111827">Overseer mean</text>
<rect x="{width - 250}" y="114" width="20" height="14" fill="#dc2626" rx="3" />
<text x="{width - 220}" y="125" font-size="12" fill="#111827">Manual mean</text>
<line x1="{width - 250}" y1="137" x2="{width - 230}" y2="137" stroke="#111827" stroke-width="3" stroke-dasharray="6 4" />
<text x="{width - 220}" y="141" font-size="12" fill="#111827">P95</text>
</svg>"""
    path.write_text(svg, encoding="utf-8")
    return path


def _print_summary(summary: Dict[str, Any]) -> None:
    print("\n" + "=" * 80)
    print("COMPARATIVE MTTR SUMMARY")
    print("-" * 80)
    for mode in ("overseer", "manual"):
        if mode not in summary["modes"]:
            continue
        mode_summary = summary["modes"][mode]
        print(f"{mode.title()}:")
        print(f"  Trials       : {mode_summary['trial_count']}")
        print(f"  Successes    : {mode_summary['success_count']}")
        print(f"  Failures     : {mode_summary['failure_count']}")
        print(f"  Mean MTTR    : {mode_summary['mean_mttr_seconds']:.2f}s +/- {mode_summary['std_mttr_seconds']:.2f}s")
        print(
            f"  Minimum MTTR : {mode_summary['min_mttr_seconds']:.2f}s"
            if mode_summary["min_mttr_seconds"] is not None
            else "  Minimum MTTR : n/a"
        )
        print(
            f"  Maximum MTTR : {mode_summary['max_mttr_seconds']:.2f}s"
            if mode_summary["max_mttr_seconds"] is not None
            else "  Maximum MTTR : n/a"
        )
        print(
            f"  P95 MTTR     : {mode_summary['p95_mttr_seconds']:.2f}s"
            if mode_summary["p95_mttr_seconds"] is not None
            else "  P95 MTTR     : n/a"
        )
    ratio = summary.get("comparison", {}).get("manual_over_overseer_ratio")
    if ratio:
        print(f"Manual / Overseer mean MTTR ratio: {ratio:.2f}x")
    print("=" * 80 + "\n")


def run_mttr_benchmark(
    *,
    trials: int = DEFAULT_TRIALS,
    agent_class: str = DEFAULT_AGENT_CLASS,
    victim_prefix: str = DEFAULT_AGENT_PREFIX,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
    timeout_seconds: float = DEFAULT_RECOVERY_TIMEOUT_SECONDS,
    inter_trial_delay_seconds: float = DEFAULT_INTER_TRIAL_DELAY_SECONDS,
    manual_operator_delay_mode: str = DEFAULT_MANUAL_OPERATOR_DELAY_MODE,
    manual_operator_delay_seconds: float = DEFAULT_MANUAL_OPERATOR_DELAY_SECONDS,
    manual_operator_delay_min_seconds: float = DEFAULT_MANUAL_OPERATOR_DELAY_MIN_SECONDS,
    manual_operator_delay_max_seconds: float = DEFAULT_MANUAL_OPERATOR_DELAY_MAX_SECONDS,
    seed: int | None = None,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
    mode: str = DEFAULT_MODE,
) -> Dict[str, Any]:
    if mode not in VALID_MODES:
        raise ValueError(f"Unsupported MTTR benchmark mode: {mode}")
    if manual_operator_delay_mode not in VALID_OPERATOR_DELAY_MODES:
        raise ValueError(
            f"Unsupported manual operator delay mode: {manual_operator_delay_mode}"
        )

    logger.info("=== COMPARATIVE MTTR RECOVERY BENCHMARK ===")
    logger.info(
        "This benchmark compares Overseer recovery against manual recovery using the same managed deploy/delete/recover path."
    )

    selected_modes = ["overseer", "manual"] if mode == "compare" else [mode]
    output_dir = Path(output_root) / _timestamp_slug()
    output_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)

    trials_by_mode: Dict[str, List[Dict[str, Any]]] = {}
    for active_mode in selected_modes:
        mode_trials: List[Dict[str, Any]] = []
        for trial_idx in range(1, trials + 1):
            logger.info(f"[MTTR/{active_mode}] Starting trial {trial_idx}/{trials}")
            try:
                result = _run_mttr_trial(
                    mode=active_mode,
                    agent_class=agent_class,
                    victim_prefix=victim_prefix,
                    poll_interval_seconds=poll_interval_seconds,
                    timeout_seconds=timeout_seconds,
                    manual_operator_delay_seconds=manual_operator_delay_seconds,
                    manual_operator_delay_mode=manual_operator_delay_mode,
                    manual_operator_delay_min_seconds=manual_operator_delay_min_seconds,
                    manual_operator_delay_max_seconds=manual_operator_delay_max_seconds,
                    rng=rng,
                )
            except Exception as exc:
                logger.error(f"[MTTR/{active_mode}] Trial {trial_idx} failed: {exc}")
                result = {
                    "mode": active_mode,
                    "success": False,
                    "victim_name": _victim_name(victim_prefix),
                    "agent_class": agent_class,
                    "mttr_seconds": None,
                    "observed_states": [],
                    "final_observed_status": "failed",
                    "final_health_status": "unknown",
                    "final_recovery_state": "failed",
                    "probe_response": None,
                    "operator_delay_seconds": None,
                    "error": str(exc),
                }
            mode_trials.append(result)
            if trial_idx < trials:
                logger.info(
                    f"[MTTR/{active_mode}] waiting {inter_trial_delay_seconds:.1f}s before next trial"
                )
                time.sleep(max(0.0, inter_trial_delay_seconds))
        trials_by_mode[active_mode] = mode_trials

    summary = _build_summary(trials_by_mode)
    csv_path = _write_trials_csv(trials_by_mode, output_dir)
    json_path = _write_summary_json(summary, trials_by_mode, output_dir, selected_mode=mode)
    chart_path = _build_svg_chart(summary, output_dir)

    _print_summary(summary)
    logger.success(f"Saved raw trials to {csv_path}")
    logger.success(f"Saved summary JSON to {json_path}")
    logger.success(f"Saved chart to {chart_path}")

    return {
        "mode": mode,
        "summary": summary,
        "trials": trials_by_mode,
        "artifacts": {
            "output_dir": str(output_dir),
            "trials_csv": str(csv_path),
            "summary_json": str(json_path),
            "chart_svg": str(chart_path),
        },
        "config": {
            "trials": trials,
            "agent_class": agent_class,
            "victim_prefix": victim_prefix,
            "poll_interval_seconds": poll_interval_seconds,
            "timeout_seconds": timeout_seconds,
            "inter_trial_delay_seconds": inter_trial_delay_seconds,
            "manual_operator_delay_mode": manual_operator_delay_mode,
            "manual_operator_delay_seconds": manual_operator_delay_seconds,
            "manual_operator_delay_min_seconds": manual_operator_delay_min_seconds,
            "manual_operator_delay_max_seconds": manual_operator_delay_max_seconds,
            "seed": seed,
        },
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the comparative MTTR recovery benchmark.")
    parser.add_argument("--trials", type=int, default=DEFAULT_TRIALS, help="Number of recovery trials per mode.")
    parser.add_argument("--agent-class", default=DEFAULT_AGENT_CLASS, help="Agent class to deploy for each trial.")
    parser.add_argument("--victim-prefix", default=DEFAULT_AGENT_PREFIX, help="Name prefix for trial deployments.")
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL_SECONDS, help="Polling interval in seconds.")
    parser.add_argument("--timeout", type=float, default=DEFAULT_RECOVERY_TIMEOUT_SECONDS, help="Per-trial recovery timeout in seconds.")
    parser.add_argument(
        "--inter-trial-delay",
        type=float,
        default=DEFAULT_INTER_TRIAL_DELAY_SECONDS,
        help="Delay in seconds between trials to let Overseer, Serve, and catalog state settle.",
    )
    parser.add_argument(
        "--manual-operator-delay-mode",
        choices=sorted(VALID_OPERATOR_DELAY_MODES),
        default=DEFAULT_MANUAL_OPERATOR_DELAY_MODE,
        help="How to model operator reaction time for manual recovery trials.",
    )
    parser.add_argument(
        "--manual-operator-delay",
        type=float,
        default=DEFAULT_MANUAL_OPERATOR_DELAY_SECONDS,
        help="Fixed operator delay in seconds when using fixed delay mode.",
    )
    parser.add_argument(
        "--manual-operator-delay-min",
        type=float,
        default=DEFAULT_MANUAL_OPERATOR_DELAY_MIN_SECONDS,
        help="Minimum operator delay in seconds when using uniform delay mode.",
    )
    parser.add_argument(
        "--manual-operator-delay-max",
        type=float,
        default=DEFAULT_MANUAL_OPERATOR_DELAY_MAX_SECONDS,
        help="Maximum operator delay in seconds when using uniform delay mode.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for reproducible operator delay sampling.",
    )
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_ROOT), help="Root directory for benchmark artifacts.")
    parser.add_argument("--mode", choices=sorted(VALID_MODES), default=DEFAULT_MODE, help="Benchmark mode to run.")
    parser.add_argument("--json", action="store_true", help="Print the final benchmark payload as JSON.")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        args = _parse_args()
        result = run_mttr_benchmark(
            trials=args.trials,
            agent_class=args.agent_class,
            victim_prefix=args.victim_prefix,
            poll_interval_seconds=args.poll_interval,
            timeout_seconds=args.timeout,
            inter_trial_delay_seconds=args.inter_trial_delay,
            manual_operator_delay_mode=args.manual_operator_delay_mode,
            manual_operator_delay_seconds=args.manual_operator_delay,
            manual_operator_delay_min_seconds=args.manual_operator_delay_min,
            manual_operator_delay_max_seconds=args.manual_operator_delay_max,
            seed=args.seed,
            output_root=args.output_dir,
            mode=args.mode,
        )
        if args.json:
            print(json.dumps(result, indent=2))
    except KeyboardInterrupt:
        logger.info("Benchmark interrupted.")
