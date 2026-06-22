#!/usr/bin/env python3
import json
import sys
from typing import NoReturn, TypeAlias, cast


REQUIRED_FIELDS = {
    "schema_version": int,
    "generated_at": int,
    "total_ops": int,
    "decode_path_ops": int,
    "decode_path_protocol_errors": int,
    "decode_path_other_errors": int,
    "decode_path_latency_max_us": (int, float),
    "decode_path_p99_us": (int, float),
    "decode_path_ops_per_sec": (int, float),
    "command_path_ops": int,
    "command_path_protocol_errors": int,
    "command_path_other_errors": int,
    "command_path_latency_max_us": (int, float),
    "command_path_p99_us": (int, float),
    "command_path_ops_per_sec": (int, float),
    "ops": int,
    "commands_started": int,
    "commands_completed": int,
    "protocol_errors": int,
    "other_errors": int,
    "latency_max_us": (int, float),
    "p99_us": (int, float),
    "ops_per_sec": (int, float),
}

JsonObject: TypeAlias = dict[str, object]


def fail(message: str) -> NoReturn:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def to_str_object_dict(raw: object) -> JsonObject:
    if not isinstance(raw, dict):
        fail("metrics line must decode to JSON object")
    raw_dict = cast(dict[object, object], raw)
    out: JsonObject = {}
    for key, value in raw_dict.items():
        out[str(key)] = value
    return out


def load_v2_line(path: str) -> JsonObject:
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            payload_obj = cast(object, json.loads(line))
            payload = to_str_object_dict(payload_obj)
            if payload.get("schema_version") == 2:
                return payload
    fail("no schema_version=2 metrics line found")


def validate_schema(payload: JsonObject) -> None:
    for key, expected in REQUIRED_FIELDS.items():
        if key not in payload:
            fail(f"schema validation failed: missing field '{key}'")
        if not isinstance(payload[key], expected):
            fail(
                f"schema validation failed: field '{key}' has invalid type {type(payload[key]).__name__}"
            )

    schema_version = payload["schema_version"]
    if not isinstance(schema_version, int):
        fail("schema validation failed: schema_version must be integer")
    if schema_version != 2:
        fail(f"schema validation failed: schema_version must be 2, got {schema_version}")

    generated_at = payload["generated_at"]
    if not isinstance(generated_at, int):
        fail("schema validation failed: generated_at must be integer")
    if generated_at <= 0:
        fail("schema validation failed: generated_at must be > 0")


def main() -> None:
    if len(sys.argv) != 2:
        fail("usage: validate_stress_metrics.py <metrics-artifact-path>")
    path = sys.argv[1]

    payload = load_v2_line(path)
    validate_schema(payload)

    print(
        f"stress metrics schema validation passed: schema_version={payload['schema_version']} generated_at={payload['generated_at']}"
    )


if __name__ == "__main__":
    main()
