#!/usr/bin/env python3
import difflib
import json
import os
import sys
from typing import NoReturn, cast


TARGET_KEYS = ("p99_us", "latency_max_us", "ops_per_sec")


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
        raw = cast(object, json.load(f))
    return to_str_object_dict(raw, "baseline")


def as_float(value: object, key: str) -> float:
    if not isinstance(value, (int, float)):
        fail(f"metrics field '{key}' must be numeric")
    return float(value)


def pick_runner_threshold(
    baseline_doc: dict[str, object],
    runner: str,
) -> tuple[dict[str, object], str]:
    runners_obj = to_str_object_dict(baseline_doc.get("runners"), "baseline.runners")
    selected_name = runner
    selected = runners_obj.get(runner)
    if selected is None:
        selected_name = "default"
        selected = runners_obj.get("default")
    if selected is None:
        fail(f"runner profile '{runner}' and fallback 'default' are both missing")
    selected_dict = to_str_object_dict(selected, f"runners.{selected_name}")
    threshold_obj = to_str_object_dict(
        selected_dict.get("max_regression_pct"),
        f"runners.{selected_name}.max_regression_pct",
    )
    return threshold_obj, selected_name


def build_updated_baseline(
    baseline_doc: dict[str, object],
    metrics: dict[str, object],
    runner: str,
) -> dict[str, object]:
    updated = dict(baseline_doc)
    runners = to_str_object_dict(updated.get("runners"), "baseline.runners")
    threshold, source_profile = pick_runner_threshold(updated, runner)

    baseline_values: dict[str, object] = {}
    for key in TARGET_KEYS:
        baseline_values[key] = round(as_float(metrics.get(key), key), 2)

    runners[runner] = {
        "baseline": baseline_values,
        "max_regression_pct": threshold,
        "source_profile": source_profile,
        "updated_from": os.environ.get("DOPE_REDIS_STRESS_RUNNER", runner),
    }
    updated["runners"] = runners
    return updated


def print_diff(before: str, after: str, baseline_path: str) -> None:
    diff = difflib.unified_diff(
        before.splitlines(keepends=True),
        after.splitlines(keepends=True),
        fromfile=f"{baseline_path} (before)",
        tofile=f"{baseline_path} (after)",
    )
    text = "".join(diff)
    if text:
        print(text, end="")
    else:
        print("no baseline diff")


def main() -> None:
    args = sys.argv[1:]
    if not (3 <= len(args) <= 4):
        fail(
            "usage: regenerate_stress_baseline.py <metrics-jsonl> <baseline-json> <runner> [--write]"
        )

    metrics_path = args[0]
    baseline_path = args[1]
    runner = args[2]
    write = len(args) == 4 and args[3] == "--write"
    if len(args) == 4 and not write:
        fail("optional fourth argument must be --write")

    metrics = load_v2_metrics(metrics_path)
    baseline_doc = load_baseline(baseline_path)

    with open(baseline_path, "r", encoding="utf-8") as f:
        before_text = f.read()

    updated = build_updated_baseline(baseline_doc, metrics, runner)
    after_text = json.dumps(updated, indent=2, sort_keys=True) + "\n"
    print_diff(before_text, after_text, baseline_path)

    if write:
        with open(baseline_path, "w", encoding="utf-8") as f:
            _ = f.write(after_text)
        print(f"updated baseline written to {baseline_path} for runner={runner}")
    else:
        print("review-only mode: baseline file unchanged (pass --write to apply)")


if __name__ == "__main__":
    main()
