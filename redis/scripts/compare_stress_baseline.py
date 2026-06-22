#!/usr/bin/env python3
import json
import os
import sys
from typing import NoReturn, cast


LOWER_IS_BETTER = ("p99_us", "latency_max_us")
HIGHER_IS_BETTER = ("ops_per_sec",)


def fail(message: str) -> NoReturn:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def to_str_object_dict(raw: object, label: str) -> dict[str, object]:
    if not isinstance(raw, dict):
        fail(f"{label} must be a JSON object")
    raw_dict = cast(dict[object, object], raw)
    out: dict[str, object] = {}
    for key, value in raw_dict.items():
        out[str(key)] = value
    return out


def load_v2_metrics(path: str) -> dict[str, object]:
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            data_obj = cast(object, json.loads(line))
            parsed = to_str_object_dict(data_obj, "metrics line")
            if parsed.get("schema_version") == 2:
                return parsed
    fail("no schema_version=2 metrics line found")


def load_baseline(path: str) -> dict[str, object]:
    with open(path, "r", encoding="utf-8") as f:
        data_obj = cast(object, json.load(f))
    return to_str_object_dict(data_obj, "baseline")


def as_float_map(node: object, label: str) -> dict[str, float]:
    node_dict = to_str_object_dict(node, label)
    out: dict[str, float] = {}
    for key, v in node_dict.items():
        if not isinstance(v, (int, float)):
            fail(f"{label}.{key} must be numeric")
        out[key] = float(v)
    return out


def select_runner_config(
    baseline_doc: dict[str, object], runner: str
) -> tuple[dict[str, float], dict[str, float], str]:
    runners_obj = to_str_object_dict(baseline_doc.get("runners"), "baseline.runners")

    selected_obj = runners_obj.get(runner)
    selected_name = runner
    if selected_obj is None:
        selected_obj = runners_obj.get("default")
        selected_name = "default"
    if selected_obj is None:
        fail(f"baseline runner config not found for '{runner}' and default fallback missing")
    selected_dict = to_str_object_dict(selected_obj, f"runners.{selected_name}")

    baseline = as_float_map(selected_dict.get("baseline"), f"runners.{selected_name}.baseline")
    max_regression_pct = as_float_map(
        selected_dict.get("max_regression_pct"),
        f"runners.{selected_name}.max_regression_pct",
    )
    return baseline, max_regression_pct, selected_name


def get_metric(metrics: dict[str, object], key: str) -> float:
    value = metrics.get(key)
    if not isinstance(value, (int, float)):
        fail(f"metrics field '{key}' must be numeric")
    return float(value)


def compare(
    current: dict[str, object],
    baseline: dict[str, float],
    max_regression_pct: dict[str, float],
) -> None:
    for key in LOWER_IS_BETTER:
        base = baseline.get(key)
        drift = max_regression_pct.get(key)
        if base is None or drift is None:
            fail(f"missing lower-is-better baseline/drift for '{key}'")
        observed = get_metric(current, key)
        upper = base * (1.0 + drift / 100.0)
        if observed > upper:
            fail(
                f"baseline-relative threshold failed: {key}={observed:.2f} > allowed={upper:.2f} (baseline={base:.2f}, max_regression_pct={drift:.2f})"
            )

    for key in HIGHER_IS_BETTER:
        base = baseline.get(key)
        drift = max_regression_pct.get(key)
        if base is None or drift is None:
            fail(f"missing higher-is-better baseline/drift for '{key}'")
        observed = get_metric(current, key)
        lower = base * (1.0 - drift / 100.0)
        if observed < lower:
            fail(
                f"baseline-relative threshold failed: {key}={observed:.2f} < allowed={lower:.2f} (baseline={base:.2f}, max_regression_pct={drift:.2f})"
            )


def main() -> None:
    if len(sys.argv) != 3:
        fail("usage: compare_stress_baseline.py <metrics-jsonl> <baseline-json>")
    metrics_path = sys.argv[1]
    baseline_path = sys.argv[2]
    runner = os.environ.get("DOPE_REDIS_STRESS_RUNNER", "default")

    current = load_v2_metrics(metrics_path)
    baseline_doc = load_baseline(baseline_path)
    baseline, max_regression_pct, selected = select_runner_config(baseline_doc, runner)
    compare(current, baseline, max_regression_pct)

    message = (
        "baseline-relative stress threshold passed: "
        + f"runner={runner} profile={selected} "
        + f"p99_us={get_metric(current, 'p99_us'):.2f} "
        + f"latency_max_us={get_metric(current, 'latency_max_us'):.2f} "
        + f"ops_per_sec={get_metric(current, 'ops_per_sec'):.2f}"
    )
    print(message)


if __name__ == "__main__":
    main()
