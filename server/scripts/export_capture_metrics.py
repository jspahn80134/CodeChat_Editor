#!/usr/bin/env python3
"""Export dissertation-oriented metrics from CodeChat capture events.

Default use pulls events directly from PostgreSQL using `capture_config.json`
or the `CODECHAT_CAPTURE_*` environment variables:

    python server/scripts/export_capture_metrics.py --out capture-metrics.csv

To produce the richer analysis dataset:

    python server/scripts/export_capture_metrics.py --dataset-dir capture-analysis

The optional positional `input` is only for fallback JSONL logs:

    python server/scripts/export_capture_metrics.py capture-events-fallback.jsonl --out capture-metrics.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator


EVENT_FIELDS = [
    "session_start",
    "session_end",
    "write_doc",
    "write_code",
    "doc_session",
    "switch_pane",
    "save",
    "compile",
    "compile_end",
    "run",
    "run_end",
    "task_start",
    "task_submit",
    "debug_task_start",
    "debug_task_submit",
    "handoff_start",
    "handoff_end",
    "reflection_prompt_inserted",
]

IDENTITY_FIELDS = [
    "user_id",
    "session_id",
]

EVENT_ROW_FIELDS = [
    "event_index",
    *IDENTITY_FIELDS,
    "event_id",
    "sequence_number",
    "schema_version",
    "event_source",
    "event_type",
    "timestamp",
    "client_timestamp_ms",
    "server_timestamp_ms",
    "client_tz_offset_min",
    "client_server_latency_ms",
    "elapsed_session_seconds",
    "gap_seconds",
    "file_id",
    "file_hash",
    "path_privacy",
    "language_id",
    "classification_basis",
    "write_source",
    "mode",
    "activity_from",
    "activity_to",
    "duration_seconds",
    "duration_ms",
    "line_count",
    "prompt_hash",
    "prompt_length",
    "command",
    "task_name",
    "task_source",
    "exit_code",
    "run_session_name",
    "run_session_type",
    "save_reason",
    "doc_block_count_before",
    "doc_block_count_after",
    "diff_hunks",
    "diff_inserted_chars",
    "diff_deleted_units",
    "diff_replacement_hunks",
    "doc_block_transactions",
    "doc_block_diff_hunks",
    "doc_block_inserted_chars",
    "doc_block_deleted_units",
]

SESSION_SUMMARY_FIELDS = [
    *IDENTITY_FIELDS,
    "event_count",
    "first_event_at",
    "last_event_at",
    "active_span_seconds",
    "events_per_minute",
    "mean_gap_seconds",
    "max_gap_seconds",
    "doc_session_seconds",
    "doc_session_share_of_span",
    "write_events",
    "doc_write_share",
    *[f"{event_type}_events" for event_type in EVENT_FIELDS],
    "doc_to_code_switches",
    "code_to_doc_switches",
    "compile_success_events",
    "compile_failure_events",
    "total_prompt_chars",
    "unique_file_count",
    "unique_language_count",
    "file_ids",
    "language_ids",
    "event_sources",
    "diff_hunks",
    "diff_inserted_chars",
    "diff_deleted_units",
    "doc_block_transactions",
    "doc_block_diff_hunks",
    "doc_block_inserted_chars",
    "doc_block_deleted_units",
    "client_server_latency_ms_mean",
    "client_server_latency_ms_max",
    "first_sequence_number",
    "last_sequence_number",
    "missing_sequence_gaps",
    "duplicate_event_ids",
    "data_quality_notes",
]

FILE_SUMMARY_FIELDS = [
    *IDENTITY_FIELDS,
    "file_id",
    "file_hash",
    "language_id",
    "path_privacy",
    "event_count",
    "first_event_at",
    "last_event_at",
    "active_span_seconds",
    "doc_session_seconds",
    "write_doc_events",
    "write_code_events",
    "save_events",
    "compile_events",
    "compile_end_events",
    "run_events",
    "run_end_events",
    "switch_pane_events",
    "line_count_first",
    "line_count_last",
    "line_count_max",
    "doc_block_count_before_min",
    "doc_block_count_after_last",
    "classification_bases",
    "write_sources",
    "diff_hunks",
    "diff_inserted_chars",
    "diff_deleted_units",
    "doc_block_transactions",
    "doc_block_diff_hunks",
    "doc_block_inserted_chars",
    "doc_block_deleted_units",
]

TASK_LIFECYCLE_FIELDS = [
    *IDENTITY_FIELDS,
    "lifecycle_kind",
    "lifecycle_index",
    "completed",
    "start_event_type",
    "end_event_type",
    "start_at",
    "end_at",
    "duration_seconds",
    "start_event_id",
    "end_event_id",
    "start_file_id",
    "end_file_id",
    "language_id",
    "command",
    "data_quality_notes",
]

LIFECYCLE_PAIRS = {
    "task_start": ("task", "task_submit"),
    "debug_task_start": ("debug_task", "debug_task_submit"),
    "handoff_start": ("handoff", "handoff_end"),
}

LIFECYCLE_END_TYPES = {
    end_type: (kind, start_type)
    for start_type, (kind, end_type) in LIFECYCLE_PAIRS.items()
}

RAW_FILE_PATH_FIELD = "file_path"

DB_METADATA_FIELDS = [
    "event_id",
    "sequence_number",
    "schema_version",
    "session_id",
    "event_source",
    "language_id",
    "file_hash",
    "path_privacy",
    "client_timestamp_ms",
    "client_tz_offset_min",
    "server_timestamp_ms",
]

FIELD_DESCRIPTIONS = {
    "event_index": "One-based event order after sorting by timestamp and sequence number.",
    "user_id": "Pseudonymous participant UUID generated or supplied by the VS Code extension.",
    "session_id": "Capture session UUID emitted by the VS Code extension.",
    "event_id": "Client-generated UUID when available.",
    "sequence_number": "Client-side monotonically increasing sequence number when available.",
    "schema_version": "Capture payload schema version.",
    "event_source": "Capture source, such as vscode_extension.",
    "event_type": "Canonical CodeChat capture event type.",
    "timestamp": "Server-recorded event timestamp.",
    "client_timestamp_ms": "Client-side timestamp in milliseconds since Unix epoch.",
    "server_timestamp_ms": "Server-side timestamp in milliseconds since Unix epoch.",
    "client_tz_offset_min": "Client timezone offset from JavaScript Date().getTimezoneOffset().",
    "client_server_latency_ms": "Approximate server timestamp minus client timestamp.",
    "elapsed_session_seconds": "Seconds since the first event in the same participant/session row.",
    "gap_seconds": "Seconds since the prior event in the same participant/session row.",
    "file_id": "Privacy-preserving file identifier. Uses captured file hash when available, otherwise a SHA-256 hash of the captured path.",
    "file_hash": "Captured SHA-256 file path hash when the extension supplied one.",
    "file_path": "Raw captured file path. Only exported with --include-file-paths.",
    "path_privacy": "Path privacy mode reported by capture settings.",
    "language_id": "VS Code language identifier when available.",
    "classification_basis": "Server-side write-classification basis when available.",
    "write_source": "Write event source, such as server_translation or CodeMirror update path.",
    "mode": "Event-specific mode or CodeChat lexer mode.",
    "activity_from": "Previous activity kind for switch_pane events.",
    "activity_to": "New activity kind for switch_pane events.",
    "duration_seconds": "Event-specific duration in seconds.",
    "duration_ms": "Event-specific duration in milliseconds.",
    "line_count": "Document line count captured on save events.",
    "prompt_hash": "SHA-256 hash of the inserted reflection prompt.",
    "prompt_length": "Length of the inserted reflection prompt.",
    "command": "Lifecycle command name recorded by the extension.",
    "task_name": "VS Code task name for compile/build events.",
    "task_source": "VS Code task source for compile/build events.",
    "exit_code": "Compile/build process exit code when available.",
    "run_session_name": "VS Code debug/run session name.",
    "run_session_type": "VS Code debug/run session type.",
    "save_reason": "Save reason reported by the extension.",
    "doc_block_count_before": "Documentation block count before a classified doc-block edit.",
    "doc_block_count_after": "Documentation block count after a classified doc-block edit.",
    "diff_hunks": "Number of text diff hunks in the event payload.",
    "diff_inserted_chars": "Characters inserted across text diff hunks.",
    "diff_deleted_units": "UTF-16 code units removed across text diff hunks.",
    "diff_replacement_hunks": "Text diff hunks that both removed and inserted content.",
    "doc_block_transactions": "Number of doc-block add/update/delete transactions.",
    "doc_block_diff_hunks": "Nested text diff hunks inside doc-block transactions.",
    "doc_block_inserted_chars": "Characters inserted inside doc-block transaction diffs.",
    "doc_block_deleted_units": "UTF-16 code units removed inside doc-block transaction diffs.",
    "event_count": "Number of events in the aggregate row.",
    "first_event_at": "Earliest event timestamp in the aggregate row.",
    "last_event_at": "Latest event timestamp in the aggregate row.",
    "active_span_seconds": "Seconds between first and last event in the aggregate row.",
    "events_per_minute": "Event count divided by active span in minutes.",
    "mean_gap_seconds": "Mean within-session gap between consecutive timestamped events.",
    "max_gap_seconds": "Largest within-session gap between consecutive timestamped events.",
    "doc_session_seconds": "Total duration of doc_session events.",
    "doc_session_share_of_span": "doc_session_seconds divided by active_span_seconds.",
    "write_events": "write_doc_events plus write_code_events.",
    "doc_write_share": "write_doc_events divided by all write events.",
    "doc_to_code_switches": "switch_pane events moving from documentation to code.",
    "code_to_doc_switches": "switch_pane events moving from code to documentation.",
    "compile_success_events": "compile_end events with exit_code equal to 0.",
    "compile_failure_events": "compile_end events with nonzero exit_code.",
    "total_prompt_chars": "Sum of reflection prompt lengths.",
    "unique_file_count": "Number of distinct file_id values in the aggregate row.",
    "unique_language_count": "Number of distinct language_id values in the aggregate row.",
    "file_ids": "Semicolon-delimited file_id values in the aggregate row.",
    "language_ids": "Semicolon-delimited language_id values in the aggregate row.",
    "event_sources": "Semicolon-delimited event_source values in the aggregate row.",
    "client_server_latency_ms_mean": "Mean approximate client-to-server timestamp delta.",
    "client_server_latency_ms_max": "Largest approximate client-to-server timestamp delta.",
    "first_sequence_number": "Smallest captured sequence number in the aggregate row.",
    "last_sequence_number": "Largest captured sequence number in the aggregate row.",
    "missing_sequence_gaps": "Count of missing sequence-number slots within the aggregate row.",
    "duplicate_event_ids": "Number of repeated event_id values in the aggregate row.",
    "data_quality_notes": "Semicolon-delimited notes about missing or suspicious capture metadata.",
    "line_count_first": "First observed line count for the file aggregate.",
    "line_count_last": "Last observed line count for the file aggregate.",
    "line_count_max": "Maximum observed line count for the file aggregate.",
    "doc_block_count_before_min": "Minimum observed doc block count before edits.",
    "doc_block_count_after_last": "Last observed doc block count after edits.",
    "classification_bases": "Semicolon-delimited write classification bases.",
    "write_sources": "Semicolon-delimited write event sources.",
    "lifecycle_kind": "Lifecycle family: task, debug_task, or handoff.",
    "lifecycle_index": "One-based lifecycle index within participant/session/task/kind.",
    "completed": "1 when a lifecycle end event was observed, otherwise 0.",
    "start_event_type": "Lifecycle start event type.",
    "end_event_type": "Lifecycle end/submit event type.",
    "start_at": "Lifecycle start timestamp.",
    "end_at": "Lifecycle end timestamp.",
    "start_event_id": "event_id for the lifecycle start event.",
    "end_event_id": "event_id for the lifecycle end event.",
    "start_file_id": "file_id associated with the lifecycle start event.",
    "end_file_id": "file_id associated with the lifecycle end event.",
}


@dataclass(frozen=True)
class DbConfig:
    host: str
    user: str
    password: str
    dbname: str
    port: int | None = None


def parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def as_data(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def normalize_db_record(record: dict[str, Any]) -> dict[str, Any]:
    data = as_data(record.get("data"))
    for field_name in DB_METADATA_FIELDS:
        value = record.get(field_name)
        if value is not None and data.get(field_name) is None:
            data[field_name] = value

    return {
        "user_id": record.get("user_id"),
        "file_path": record.get("file_path"),
        "event_type": record.get("event_type"),
        "timestamp": record.get("timestamp"),
        "data": data,
    }


def iter_jsonl_events(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as input_file:
        for line_number, line in enumerate(input_file, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as err:
                raise SystemExit(f"{path}:{line_number}: invalid JSON: {err}") from err
            event = record.get("event", record)
            if not isinstance(event, dict):
                continue
            event["data"] = as_data(event.get("data"))
            yield event


def load_db_config(config_path: Path) -> DbConfig:
    env_config = db_config_from_env()
    if env_config is not None:
        return env_config

    config_path = resolve_config_path(config_path)
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except FileNotFoundError as err:
        searched = "\n    ".join(str(path) for path in config_search_paths(config_path))
        raise SystemExit(
            "No DB config found. Create a local capture_config.json, set "
            "CODECHAT_CAPTURE_* env vars, or pass a fallback JSONL input file.\n"
            f"Searched:\n    {searched}"
        ) from err
    except json.JSONDecodeError as err:
        raise SystemExit(f"{config_path}: invalid JSON: {err}") from err

    missing = [name for name in ["host", "user", "password", "dbname"] if not config.get(name)]
    if missing:
        raise SystemExit(f"{config_path}: missing required DB field(s): {', '.join(missing)}")

    return DbConfig(
        host=str(config["host"]),
        user=str(config["user"]),
        password=str(config["password"]),
        dbname=str(config["dbname"]),
        port=int(config["port"]) if config.get("port") is not None else None,
    )


def resolve_config_path(config_path: Path) -> Path:
    for candidate in config_search_paths(config_path):
        if candidate.exists():
            return candidate
    return config_path


def config_search_paths(config_path: Path) -> list[Path]:
    if config_path.is_absolute():
        return [config_path]

    script_repo_root = Path(__file__).resolve().parents[2]
    paths = [Path.cwd() / config_path, script_repo_root / config_path]

    unique_paths: list[Path] = []
    for path in paths:
        if path not in unique_paths:
            unique_paths.append(path)
    return unique_paths


def db_config_from_env() -> DbConfig | None:
    host = env_value("CODECHAT_CAPTURE_HOST")
    if host is None:
        return None
    missing = [
        name
        for name in [
            "CODECHAT_CAPTURE_USER",
            "CODECHAT_CAPTURE_PASSWORD",
            "CODECHAT_CAPTURE_DBNAME",
        ]
        if env_value(name) is None
    ]
    if missing:
        raise SystemExit(
            "Missing required capture DB environment variable(s): " + ", ".join(missing)
        )

    port_text = env_value("CODECHAT_CAPTURE_PORT")
    return DbConfig(
        host=host,
        user=env_value("CODECHAT_CAPTURE_USER") or "",
        password=env_value("CODECHAT_CAPTURE_PASSWORD") or "",
        dbname=env_value("CODECHAT_CAPTURE_DBNAME") or "",
        port=int(port_text) if port_text is not None else None,
    )


def env_value(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def sql_identifier(identifier: str) -> str:
    parts = identifier.split(".")
    for part in parts:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part):
            raise SystemExit(f"Invalid SQL identifier: {identifier!r}")
    return ".".join(f'"{part}"' for part in parts)


def iter_db_events(config: DbConfig, table: str) -> Iterator[dict[str, Any]]:
    try:
        import psycopg
    except ImportError:
        yield from iter_db_events_with_psql(config, table)
        return

    connect_kwargs = {
        "host": config.host,
        "user": config.user,
        "password": config.password,
        "dbname": config.dbname,
    }
    if config.port is not None:
        connect_kwargs["port"] = config.port

    query = psql_json_query(table)
    with psycopg.connect(**connect_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            for (record_text,) in cursor:
                yield normalize_db_record(json.loads(record_text))


def iter_db_events_with_psql(config: DbConfig, table: str) -> Iterator[dict[str, Any]]:
    psql_path = find_psql()
    if psql_path is None:
        raise SystemExit(
            "PostgreSQL export needs a local PostgreSQL client to connect to the AWS DB.\n"
            "The AWS PostgreSQL server is remote; it cannot provide Python's local DB driver.\n"
            "Install one of these on this Windows machine:\n"
            "    python -m pip install \"psycopg[binary]\"\n"
            "or install PostgreSQL command-line tools so psql.exe is available on PATH."
        )

    env = os.environ.copy()
    env["PGPASSWORD"] = config.password
    command = [
        psql_path,
        "--no-password",
        "--no-align",
        "--tuples-only",
        "--quiet",
        "--set",
        "ON_ERROR_STOP=1",
        "--host",
        config.host,
        "--username",
        config.user,
        "--dbname",
        config.dbname,
        "--command",
        psql_json_query(table),
    ]
    if config.port is not None:
        command.extend(["--port", str(config.port)])

    result = subprocess.run(
        command,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit(
            "psql failed while querying the AWS PostgreSQL DB:\n"
            f"{result.stderr.strip() or result.stdout.strip()}"
        )

    for line_number, line in enumerate(result.stdout.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as err:
            raise SystemExit(f"psql output line {line_number}: invalid JSON: {err}") from err
        yield normalize_db_record(record)


def find_psql() -> str | None:
    psql_path = shutil.which("psql")
    if psql_path is not None:
        return psql_path

    program_files = Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
    candidates = sorted(program_files.glob(r"PostgreSQL/*/bin/psql.exe"), reverse=True)
    return str(candidates[0]) if candidates else None


def psql_json_query(table: str) -> str:
    return (
        "SELECT to_jsonb(events_row)::text "
        f"FROM {sql_identifier(table)} AS events_row "
        'ORDER BY events_row."timestamp"'
    )


def text_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return str(value)


def first_data_value(data: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in data and data[name] is not None:
            return data[name]
    return None


def data_text(data: dict[str, Any], *names: str) -> str:
    return text_value(first_data_value(data, *names))


def int_value(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return int(value)
        except ValueError:
            return None
    return None


def float_value(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None
    return None


def int_or_blank(value: Any) -> int | str:
    number = int_value(value)
    return number if number is not None else ""


def float_or_blank(value: Any) -> float | str:
    number = float_value(value)
    return number if number is not None else ""


def csv_value(value: Any) -> str | int:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.3f}"
    if isinstance(value, bool):
        return "1" if value else "0"
    return value


def write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    if path.parent != Path("."):
        path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: csv_value(row.get(field, "")) for field in fieldnames})


def aware_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def seconds_between(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return (end - start).total_seconds()


def timestamp_for_csv(value: datetime | None, fallback: Any = "") -> str:
    if value is not None:
        return value.isoformat()
    return text_value(fallback)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def file_id_for(file_path: str, file_hash: str) -> str:
    if file_hash:
        return file_hash
    if file_path:
        return sha256_text(file_path)
    return ""


def fields_with_optional_file_path(
    fieldnames: list[str], include_file_paths: bool
) -> list[str]:
    if not include_file_paths or RAW_FILE_PATH_FIELD in fieldnames:
        return fieldnames
    fields = list(fieldnames)
    insert_after = "file_hash" if "file_hash" in fields else "file_id"
    fields.insert(fields.index(insert_after) + 1, RAW_FILE_PATH_FIELD)
    return fields


def identity_key(row: dict[str, Any]) -> tuple[str, ...]:
    return tuple(text_value(row.get(field)) for field in IDENTITY_FIELDS)


def semicolon_join(values: Iterable[str]) -> str:
    return ";".join(sorted(value for value in values if value))


def add_number(acc: list[float], value: Any) -> None:
    number = float_value(value)
    if number is not None:
        acc.append(number)


def string_diff_stats(value: Any) -> Counter[str]:
    stats: Counter[str] = Counter()
    if isinstance(value, list):
        for item in value:
            stats.update(string_diff_stats(item))
        return stats
    if not isinstance(value, dict):
        return stats

    if "from" in value and "insert" in value:
        from_value = int_value(value.get("from")) or 0
        to_value = int_value(value.get("to"))
        removed_units = max(0, (to_value or from_value) - from_value)
        inserted_chars = len(text_value(value.get("insert")))
        stats["hunks"] += 1
        stats["inserted_chars"] += inserted_chars
        stats["deleted_units"] += removed_units
        if removed_units > 0 and inserted_chars > 0:
            stats["replacement_hunks"] += 1
        return stats

    for child in value.values():
        stats.update(string_diff_stats(child))
    return stats


def doc_block_contents(value: Any) -> str:
    if isinstance(value, list) and len(value) >= 5:
        return text_value(value[4])
    if isinstance(value, dict):
        return text_value(value.get("contents"))
    return ""


def doc_block_diff_stats(value: Any) -> Counter[str]:
    stats: Counter[str] = Counter()
    if not isinstance(value, list):
        return stats

    for transaction in value:
        stats["transactions"] += 1
        if not isinstance(transaction, dict):
            continue
        if "Add" in transaction:
            stats["inserted_chars"] += len(doc_block_contents(transaction["Add"]))
        elif "Update" in transaction:
            update_stats = string_diff_stats(transaction["Update"])
            stats["hunks"] += update_stats["hunks"]
            stats["inserted_chars"] += update_stats["inserted_chars"]
            stats["deleted_units"] += update_stats["deleted_units"]
        elif "Delete" in transaction:
            continue
        else:
            update_stats = string_diff_stats(transaction)
            stats["hunks"] += update_stats["hunks"]
            stats["inserted_chars"] += update_stats["inserted_chars"]
            stats["deleted_units"] += update_stats["deleted_units"]
    return stats


def normalize_event_rows(
    events: Iterable[dict[str, Any]], include_file_paths: bool = False
) -> list[dict[str, Any]]:
    sortable_rows: list[dict[str, Any]] = []
    for original_index, event in enumerate(events, start=1):
        data = as_data(event.get("data"))
        timestamp = aware_datetime(parse_timestamp(event.get("timestamp")))
        file_path = text_value(event.get("file_path"))
        file_hash = data_text(data, "file_hash")
        client_timestamp_ms = int_value(data.get("client_timestamp_ms"))
        server_timestamp_ms = int_value(data.get("server_timestamp_ms"))
        latency_ms = (
            server_timestamp_ms - client_timestamp_ms
            if client_timestamp_ms is not None and server_timestamp_ms is not None
            else ""
        )
        diff_stats = string_diff_stats(data.get("diff"))
        doc_block_stats = doc_block_diff_stats(data.get("doc_block_diff"))

        row: dict[str, Any] = {
            "event_index": original_index,
            "user_id": text_value(event.get("user_id")),
            "session_id": data_text(data, "session_id"),
            "event_id": data_text(data, "event_id"),
            "sequence_number": int_or_blank(data.get("sequence_number")),
            "schema_version": int_or_blank(data.get("schema_version")),
            "event_source": data_text(data, "event_source"),
            "event_type": text_value(event.get("event_type")),
            "timestamp": timestamp_for_csv(timestamp, event.get("timestamp")),
            "client_timestamp_ms": client_timestamp_ms
            if client_timestamp_ms is not None
            else "",
            "server_timestamp_ms": server_timestamp_ms
            if server_timestamp_ms is not None
            else "",
            "client_tz_offset_min": int_or_blank(data.get("client_tz_offset_min")),
            "client_server_latency_ms": latency_ms,
            "elapsed_session_seconds": "",
            "gap_seconds": "",
            "file_id": file_id_for(file_path, file_hash),
            "file_hash": file_hash,
            "path_privacy": data_text(data, "path_privacy"),
            "language_id": data_text(data, "language_id", "languageId"),
            "classification_basis": data_text(data, "classification_basis"),
            "write_source": data_text(data, "source"),
            "mode": data_text(data, "mode"),
            "activity_from": data_text(data, "from"),
            "activity_to": data_text(data, "to"),
            "duration_seconds": float_or_blank(data.get("duration_seconds")),
            "duration_ms": float_or_blank(data.get("duration_ms")),
            "line_count": int_or_blank(first_data_value(data, "lineCount", "line_count")),
            "prompt_hash": data_text(data, "prompt_hash"),
            "prompt_length": int_or_blank(data.get("prompt_length")),
            "command": data_text(data, "command"),
            "task_name": data_text(data, "taskName", "task_name"),
            "task_source": data_text(data, "taskSource", "task_source"),
            "exit_code": int_or_blank(first_data_value(data, "exitCode", "exit_code")),
            "run_session_name": data_text(data, "sessionName", "session_name"),
            "run_session_type": data_text(data, "sessionType", "session_type"),
            "save_reason": data_text(data, "reason"),
            "doc_block_count_before": int_or_blank(data.get("doc_block_count_before")),
            "doc_block_count_after": int_or_blank(data.get("doc_block_count_after")),
            "diff_hunks": diff_stats["hunks"],
            "diff_inserted_chars": diff_stats["inserted_chars"],
            "diff_deleted_units": diff_stats["deleted_units"],
            "diff_replacement_hunks": diff_stats["replacement_hunks"],
            "doc_block_transactions": doc_block_stats["transactions"],
            "doc_block_diff_hunks": doc_block_stats["hunks"],
            "doc_block_inserted_chars": doc_block_stats["inserted_chars"],
            "doc_block_deleted_units": doc_block_stats["deleted_units"],
        }
        if include_file_paths:
            row[RAW_FILE_PATH_FIELD] = file_path

        sortable_rows.append(
            {
                "row": row,
                "timestamp": timestamp,
                "sequence_number": int_value(row["sequence_number"]),
                "original_index": original_index,
            }
        )

    max_timestamp = datetime.max.replace(tzinfo=timezone.utc)
    sortable_rows.sort(
        key=lambda item: (
            item["timestamp"] is None,
            item["timestamp"] or max_timestamp,
            item["sequence_number"] if item["sequence_number"] is not None else 10**18,
            item["original_index"],
        )
    )

    first_by_session: dict[tuple[str, ...], datetime] = {}
    previous_by_session: dict[tuple[str, ...], datetime] = {}
    for event_index, item in enumerate(sortable_rows, start=1):
        row = item["row"]
        row["event_index"] = event_index
        timestamp = item["timestamp"]
        if timestamp is None:
            continue
        key = identity_key(row)
        first = first_by_session.setdefault(key, timestamp)
        row["elapsed_session_seconds"] = seconds_between(first, timestamp) or 0.0
        previous = previous_by_session.get(key)
        if previous is not None:
            row["gap_seconds"] = seconds_between(previous, timestamp) or 0.0
        previous_by_session[key] = timestamp

    return [item["row"] for item in sortable_rows]


def new_session_acc(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "identity": {field: text_value(row.get(field)) for field in IDENTITY_FIELDS},
        "counts": Counter(),
        "event_count": 0,
        "first_dt": None,
        "last_dt": None,
        "gaps": [],
        "doc_session_seconds": 0.0,
        "doc_to_code_switches": 0,
        "code_to_doc_switches": 0,
        "compile_success_events": 0,
        "compile_failure_events": 0,
        "total_prompt_chars": 0,
        "file_ids": set(),
        "language_ids": set(),
        "event_sources": set(),
        "latencies": [],
        "sequence_numbers": [],
        "event_ids": Counter(),
        "diff_hunks": 0,
        "diff_inserted_chars": 0,
        "diff_deleted_units": 0,
        "doc_block_transactions": 0,
        "doc_block_diff_hunks": 0,
        "doc_block_inserted_chars": 0,
        "doc_block_deleted_units": 0,
        "missing_timestamp_count": 0,
        "missing_session_count": 0,
        "missing_schema_count": 0,
    }


def update_time_acc(acc: dict[str, Any], row: dict[str, Any]) -> None:
    timestamp = aware_datetime(parse_timestamp(row.get("timestamp")))
    if timestamp is None:
        acc["missing_timestamp_count"] += 1
        return
    if acc["first_dt"] is None or timestamp < acc["first_dt"]:
        acc["first_dt"] = timestamp
    if acc["last_dt"] is None or timestamp > acc["last_dt"]:
        acc["last_dt"] = timestamp


def update_session_acc(acc: dict[str, Any], row: dict[str, Any]) -> None:
    event_type = text_value(row.get("event_type"))
    acc["event_count"] += 1
    acc["counts"][event_type] += 1
    update_time_acc(acc, row)

    add_number(acc["gaps"], row.get("gap_seconds"))
    if event_type == "doc_session":
        duration = float_value(row.get("duration_seconds"))
        if duration is not None:
            acc["doc_session_seconds"] += duration
    if event_type == "switch_pane":
        if row.get("activity_from") == "doc" and row.get("activity_to") == "code":
            acc["doc_to_code_switches"] += 1
        if row.get("activity_from") == "code" and row.get("activity_to") == "doc":
            acc["code_to_doc_switches"] += 1
    if event_type == "compile_end":
        exit_code = int_value(row.get("exit_code"))
        if exit_code == 0:
            acc["compile_success_events"] += 1
        elif exit_code is not None:
            acc["compile_failure_events"] += 1

    acc["total_prompt_chars"] += int_value(row.get("prompt_length")) or 0
    if row.get("file_id"):
        acc["file_ids"].add(text_value(row.get("file_id")))
    if row.get("language_id"):
        acc["language_ids"].add(text_value(row.get("language_id")))
    if row.get("event_source"):
        acc["event_sources"].add(text_value(row.get("event_source")))
    add_number(acc["latencies"], row.get("client_server_latency_ms"))

    sequence_number = int_value(row.get("sequence_number"))
    if sequence_number is not None:
        acc["sequence_numbers"].append(sequence_number)
    event_id = text_value(row.get("event_id"))
    if event_id:
        acc["event_ids"][event_id] += 1

    for field in [
        "diff_hunks",
        "diff_inserted_chars",
        "diff_deleted_units",
        "doc_block_transactions",
        "doc_block_diff_hunks",
        "doc_block_inserted_chars",
        "doc_block_deleted_units",
    ]:
        acc[field] += int_value(row.get(field)) or 0
    if not row.get("session_id"):
        acc["missing_session_count"] += 1
    if not row.get("schema_version"):
        acc["missing_schema_count"] += 1


def finalize_session_acc(acc: dict[str, Any]) -> dict[str, Any]:
    first_dt = acc["first_dt"]
    last_dt = acc["last_dt"]
    active_span = seconds_between(first_dt, last_dt) or 0.0
    counts = acc["counts"]
    write_events = counts["write_doc"] + counts["write_code"]
    sequence_numbers = sorted(set(acc["sequence_numbers"]))
    missing_sequence_gaps = sum(
        max(0, current - previous - 1)
        for previous, current in zip(sequence_numbers, sequence_numbers[1:])
    )
    duplicate_event_ids = sum(
        count - 1 for count in acc["event_ids"].values() if count > 1
    )
    notes = []
    if acc["missing_timestamp_count"]:
        notes.append(f"missing_timestamp:{acc['missing_timestamp_count']}")
    if acc["missing_session_count"]:
        notes.append(f"missing_session_id:{acc['missing_session_count']}")
    if acc["missing_schema_count"]:
        notes.append(f"missing_schema_version:{acc['missing_schema_count']}")
    if missing_sequence_gaps:
        notes.append(f"missing_sequence_slots:{missing_sequence_gaps}")
    if duplicate_event_ids:
        notes.append(f"duplicate_event_ids:{duplicate_event_ids}")

    row = {
        **acc["identity"],
        "event_count": acc["event_count"],
        "first_event_at": timestamp_for_csv(first_dt),
        "last_event_at": timestamp_for_csv(last_dt),
        "active_span_seconds": active_span,
        "events_per_minute": (acc["event_count"] / (active_span / 60.0))
        if active_span > 0
        else "",
        "mean_gap_seconds": sum(acc["gaps"]) / len(acc["gaps"]) if acc["gaps"] else "",
        "max_gap_seconds": max(acc["gaps"]) if acc["gaps"] else "",
        "doc_session_seconds": acc["doc_session_seconds"],
        "doc_session_share_of_span": acc["doc_session_seconds"] / active_span
        if active_span > 0
        else "",
        "write_events": write_events,
        "doc_write_share": counts["write_doc"] / write_events if write_events else "",
        **{f"{event_type}_events": counts[event_type] for event_type in EVENT_FIELDS},
        "doc_to_code_switches": acc["doc_to_code_switches"],
        "code_to_doc_switches": acc["code_to_doc_switches"],
        "compile_success_events": acc["compile_success_events"],
        "compile_failure_events": acc["compile_failure_events"],
        "total_prompt_chars": acc["total_prompt_chars"],
        "unique_file_count": len(acc["file_ids"]),
        "unique_language_count": len(acc["language_ids"]),
        "file_ids": semicolon_join(acc["file_ids"]),
        "language_ids": semicolon_join(acc["language_ids"]),
        "event_sources": semicolon_join(acc["event_sources"]),
        "diff_hunks": acc["diff_hunks"],
        "diff_inserted_chars": acc["diff_inserted_chars"],
        "diff_deleted_units": acc["diff_deleted_units"],
        "doc_block_transactions": acc["doc_block_transactions"],
        "doc_block_diff_hunks": acc["doc_block_diff_hunks"],
        "doc_block_inserted_chars": acc["doc_block_inserted_chars"],
        "doc_block_deleted_units": acc["doc_block_deleted_units"],
        "client_server_latency_ms_mean": sum(acc["latencies"]) / len(acc["latencies"])
        if acc["latencies"]
        else "",
        "client_server_latency_ms_max": max(acc["latencies"]) if acc["latencies"] else "",
        "first_sequence_number": sequence_numbers[0] if sequence_numbers else "",
        "last_sequence_number": sequence_numbers[-1] if sequence_numbers else "",
        "missing_sequence_gaps": missing_sequence_gaps,
        "duplicate_event_ids": duplicate_event_ids,
        "data_quality_notes": ";".join(notes),
    }
    return row


def session_summary_rows(event_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    accs: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in event_rows:
        key = identity_key(row)
        acc = accs.setdefault(key, new_session_acc(row))
        update_session_acc(acc, row)
    return [
        finalize_session_acc(acc)
        for _, acc in sorted(accs.items(), key=lambda item: item[0])
    ]


def new_file_acc(row: dict[str, Any], include_file_paths: bool) -> dict[str, Any]:
    acc = {
        "identity": {field: text_value(row.get(field)) for field in IDENTITY_FIELDS},
        "file_id": text_value(row.get("file_id")),
        "file_hash": text_value(row.get("file_hash")),
        "language_id": text_value(row.get("language_id")),
        "path_privacy": text_value(row.get("path_privacy")),
        "counts": Counter(),
        "event_count": 0,
        "first_dt": None,
        "last_dt": None,
        "doc_session_seconds": 0.0,
        "line_count_first": "",
        "line_count_last": "",
        "line_count_max": "",
        "doc_block_count_before_min": "",
        "doc_block_count_after_last": "",
        "classification_bases": set(),
        "write_sources": set(),
        "diff_hunks": 0,
        "diff_inserted_chars": 0,
        "diff_deleted_units": 0,
        "doc_block_transactions": 0,
        "doc_block_diff_hunks": 0,
        "doc_block_inserted_chars": 0,
        "doc_block_deleted_units": 0,
    }
    if include_file_paths:
        acc[RAW_FILE_PATH_FIELD] = text_value(row.get(RAW_FILE_PATH_FIELD))
    return acc


def update_file_acc(acc: dict[str, Any], row: dict[str, Any]) -> None:
    event_type = text_value(row.get("event_type"))
    acc["event_count"] += 1
    acc["counts"][event_type] += 1
    update_time_acc(acc, row)

    if event_type == "doc_session":
        duration = float_value(row.get("duration_seconds"))
        if duration is not None:
            acc["doc_session_seconds"] += duration

    line_count = int_value(row.get("line_count"))
    if line_count is not None:
        if acc["line_count_first"] == "":
            acc["line_count_first"] = line_count
        acc["line_count_last"] = line_count
        acc["line_count_max"] = max(int_value(acc["line_count_max"]) or 0, line_count)

    before_count = int_value(row.get("doc_block_count_before"))
    if before_count is not None:
        current_min = int_value(acc["doc_block_count_before_min"])
        acc["doc_block_count_before_min"] = (
            before_count if current_min is None else min(current_min, before_count)
        )
    after_count = int_value(row.get("doc_block_count_after"))
    if after_count is not None:
        acc["doc_block_count_after_last"] = after_count

    if row.get("classification_basis"):
        acc["classification_bases"].add(text_value(row.get("classification_basis")))
    if row.get("write_source"):
        acc["write_sources"].add(text_value(row.get("write_source")))

    for field in [
        "diff_hunks",
        "diff_inserted_chars",
        "diff_deleted_units",
        "doc_block_transactions",
        "doc_block_diff_hunks",
        "doc_block_inserted_chars",
        "doc_block_deleted_units",
    ]:
        acc[field] += int_value(row.get(field)) or 0


def finalize_file_acc(acc: dict[str, Any], include_file_paths: bool) -> dict[str, Any]:
    first_dt = acc["first_dt"]
    last_dt = acc["last_dt"]
    row = {
        **acc["identity"],
        "file_id": acc["file_id"],
        "file_hash": acc["file_hash"],
        "language_id": acc["language_id"],
        "path_privacy": acc["path_privacy"],
        "event_count": acc["event_count"],
        "first_event_at": timestamp_for_csv(first_dt),
        "last_event_at": timestamp_for_csv(last_dt),
        "active_span_seconds": seconds_between(first_dt, last_dt) or 0.0,
        "doc_session_seconds": acc["doc_session_seconds"],
        "write_doc_events": acc["counts"]["write_doc"],
        "write_code_events": acc["counts"]["write_code"],
        "save_events": acc["counts"]["save"],
        "compile_events": acc["counts"]["compile"],
        "compile_end_events": acc["counts"]["compile_end"],
        "run_events": acc["counts"]["run"],
        "run_end_events": acc["counts"]["run_end"],
        "switch_pane_events": acc["counts"]["switch_pane"],
        "line_count_first": acc["line_count_first"],
        "line_count_last": acc["line_count_last"],
        "line_count_max": acc["line_count_max"],
        "doc_block_count_before_min": acc["doc_block_count_before_min"],
        "doc_block_count_after_last": acc["doc_block_count_after_last"],
        "classification_bases": semicolon_join(acc["classification_bases"]),
        "write_sources": semicolon_join(acc["write_sources"]),
        "diff_hunks": acc["diff_hunks"],
        "diff_inserted_chars": acc["diff_inserted_chars"],
        "diff_deleted_units": acc["diff_deleted_units"],
        "doc_block_transactions": acc["doc_block_transactions"],
        "doc_block_diff_hunks": acc["doc_block_diff_hunks"],
        "doc_block_inserted_chars": acc["doc_block_inserted_chars"],
        "doc_block_deleted_units": acc["doc_block_deleted_units"],
    }
    if include_file_paths:
        row[RAW_FILE_PATH_FIELD] = acc.get(RAW_FILE_PATH_FIELD, "")
    return row


def file_summary_rows(
    event_rows: Iterable[dict[str, Any]], include_file_paths: bool
) -> list[dict[str, Any]]:
    accs: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in event_rows:
        key = (
            *identity_key(row),
            text_value(row.get("file_id")),
            text_value(row.get("language_id")),
        )
        acc = accs.setdefault(key, new_file_acc(row, include_file_paths))
        update_file_acc(acc, row)
    return [
        finalize_file_acc(acc, include_file_paths)
        for _, acc in sorted(accs.items(), key=lambda item: item[0])
    ]


def lifecycle_row(
    kind: str,
    lifecycle_index: int,
    start: dict[str, Any] | None,
    end: dict[str, Any] | None,
) -> dict[str, Any]:
    source = start or end or {}
    start_dt = aware_datetime(parse_timestamp(start.get("timestamp") if start else None))
    end_dt = aware_datetime(parse_timestamp(end.get("timestamp") if end else None))
    notes = []
    if start is None:
        notes.append("missing_start")
    if end is None:
        notes.append("missing_end")
    return {
        **{field: text_value(source.get(field)) for field in IDENTITY_FIELDS},
        "lifecycle_kind": kind,
        "lifecycle_index": lifecycle_index,
        "completed": 1 if end is not None else 0,
        "start_event_type": text_value(start.get("event_type")) if start else "",
        "end_event_type": text_value(end.get("event_type")) if end else "",
        "start_at": timestamp_for_csv(start_dt),
        "end_at": timestamp_for_csv(end_dt),
        "duration_seconds": seconds_between(start_dt, end_dt)
        if start_dt is not None and end_dt is not None
        else "",
        "start_event_id": text_value(start.get("event_id")) if start else "",
        "end_event_id": text_value(end.get("event_id")) if end else "",
        "start_file_id": text_value(start.get("file_id")) if start else "",
        "end_file_id": text_value(end.get("file_id")) if end else "",
        "language_id": text_value(source.get("language_id")),
        "command": text_value(source.get("command")),
        "data_quality_notes": ";".join(notes),
    }


def task_lifecycle_rows(event_rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    starts: dict[tuple[tuple[str, ...], str], deque[dict[str, Any]]] = defaultdict(deque)
    lifecycle_indexes: Counter[tuple[tuple[str, ...], str]] = Counter()
    rows: list[dict[str, Any]] = []

    for row in event_rows:
        event_type = text_value(row.get("event_type"))
        if event_type in LIFECYCLE_PAIRS:
            kind, _ = LIFECYCLE_PAIRS[event_type]
            starts[(identity_key(row), kind)].append(row)
            continue
        if event_type not in LIFECYCLE_END_TYPES:
            continue

        kind, _start_type = LIFECYCLE_END_TYPES[event_type]
        key = (identity_key(row), kind)
        start = starts[key].popleft() if starts[key] else None
        lifecycle_indexes[key] += 1
        rows.append(lifecycle_row(kind, lifecycle_indexes[key], start, row))

    for key, queue in sorted(starts.items(), key=lambda item: item[0]):
        identity, kind = key
        while queue:
            start = queue.popleft()
            lifecycle_indexes[(identity, kind)] += 1
            rows.append(lifecycle_row(kind, lifecycle_indexes[(identity, kind)], start, None))

    rows.sort(
        key=lambda row: (
            row["user_id"],
            row["session_id"],
            row["lifecycle_kind"],
            row["lifecycle_index"],
        )
    )
    return rows


def data_dictionary_rows(fieldsets: dict[str, list[str]]) -> list[dict[str, str]]:
    rows = []
    for dataset, fields in fieldsets.items():
        for field in fields:
            rows.append(
                {
                    "dataset": dataset,
                    "column": field,
                    "description": FIELD_DESCRIPTIONS.get(field, ""),
                }
            )
    return rows


def export_metrics(event_rows: list[dict[str, Any]], output_path: Path) -> None:
    write_csv(output_path, SESSION_SUMMARY_FIELDS, session_summary_rows(event_rows))


def export_analysis_dataset(
    event_rows: list[dict[str, Any]], dataset_dir: Path, include_file_paths: bool
) -> list[Path]:
    dataset_dir.mkdir(parents=True, exist_ok=True)
    event_fields = fields_with_optional_file_path(EVENT_ROW_FIELDS, include_file_paths)
    file_fields = fields_with_optional_file_path(FILE_SUMMARY_FIELDS, include_file_paths)
    fieldsets = {
        "events.csv": event_fields,
        "session_summary.csv": SESSION_SUMMARY_FIELDS,
        "file_summary.csv": file_fields,
        "task_lifecycle.csv": TASK_LIFECYCLE_FIELDS,
    }
    outputs = [
        dataset_dir / "events.csv",
        dataset_dir / "session_summary.csv",
        dataset_dir / "file_summary.csv",
        dataset_dir / "task_lifecycle.csv",
        dataset_dir / "data_dictionary.csv",
    ]
    write_csv(outputs[0], event_fields, event_rows)
    write_csv(outputs[1], SESSION_SUMMARY_FIELDS, session_summary_rows(event_rows))
    write_csv(outputs[2], file_fields, file_summary_rows(event_rows, include_file_paths))
    write_csv(outputs[3], TASK_LIFECYCLE_FIELDS, task_lifecycle_rows(event_rows))
    write_csv(
        outputs[4],
        ["dataset", "column", "description"],
        data_dictionary_rows(fieldsets),
    )
    return outputs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        help="Optional capture JSONL fallback file. Omit to read PostgreSQL.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help=(
            "Output session-summary CSV file. Defaults to a timestamped "
            "capture-metrics-YYYYMMDD-HHMMSS.csv file when --dataset-dir is omitted."
        ),
    )
    parser.add_argument(
        "--dataset-dir",
        nargs="?",
        const=Path("__DEFAULT_CAPTURE_ANALYSIS_DIR__"),
        type=Path,
        default=None,
        help=(
            "Write a richer analysis dataset directory containing events.csv, "
            "session_summary.csv, file_summary.csv, task_lifecycle.csv, and "
            "data_dictionary.csv. If no path is supplied, defaults to "
            "capture-analysis-YYYYMMDD-HHMMSS."
        ),
    )
    parser.add_argument(
        "--include-file-paths",
        action="store_true",
        help=(
            "Include raw captured file paths in event/file exports. By default, "
            "the dataset uses file_id/file_hash only."
        ),
    )
    parser.add_argument(
        "--db",
        action="store_true",
        help="Read PostgreSQL. This is the default when no JSONL input is supplied.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("capture_config.json"),
        help="Capture DB config JSON path. Ignored when CODECHAT_CAPTURE_* env vars are set.",
    )
    parser.add_argument(
        "--table",
        default="events",
        help='Capture events table name. Defaults to "events".',
    )
    args = parser.parse_args()

    if args.db and args.input is not None:
        parser.error("do not pass a JSONL input path with --db")

    events = (
        iter_jsonl_events(args.input)
        if args.input is not None
        else iter_db_events(load_db_config(args.config), args.table)
    )
    event_rows = normalize_event_rows(events, include_file_paths=args.include_file_paths)

    wrote_outputs = False
    if args.out is not None or args.dataset_dir is None:
        output_path = args.out or default_output_path()
        export_metrics(event_rows, output_path)
        print(f"Wrote {output_path}")
        wrote_outputs = True

    if args.dataset_dir is not None:
        dataset_dir = (
            default_dataset_dir()
            if args.dataset_dir == Path("__DEFAULT_CAPTURE_ANALYSIS_DIR__")
            else args.dataset_dir
        )
        output_paths = export_analysis_dataset(
            event_rows, dataset_dir, args.include_file_paths
        )
        print(f"Wrote analysis dataset to {dataset_dir}")
        for output_path in output_paths:
            print(f"  {output_path.name}")
        wrote_outputs = True

    if not wrote_outputs:
        raise SystemExit("No outputs were requested.")


def default_output_path() -> Path:
    timestamp = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d-%H%M%S")
    return Path(f"capture-metrics-{timestamp}.csv")


def default_dataset_dir() -> Path:
    timestamp = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d-%H%M%S")
    return Path(f"capture-analysis-{timestamp}")


if __name__ == "__main__":
    main()
