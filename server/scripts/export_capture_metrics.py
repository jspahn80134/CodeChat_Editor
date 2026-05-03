#!/usr/bin/env python3
"""Export dissertation-oriented metrics from CodeChat capture events.

Default use pulls events directly from PostgreSQL using `capture_config.json`
or the `CODECHAT_CAPTURE_*` environment variables:

    python server/scripts/export_capture_metrics.py --out capture-metrics.csv

The optional positional `input` is only for fallback JSONL logs:

    python server/scripts/export_capture_metrics.py capture-events-fallback.jsonl --out capture-metrics.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator


EVENT_FIELDS = [
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


@dataclass(frozen=True)
class DbConfig:
    host: str
    user: str
    password: str
    dbname: str
    port: int | None = None


@dataclass
class MetricRow:
    user_id: str
    assignment_id: str
    group_id: str
    session_id: str
    condition: str
    course_id: str
    task_id: str
    event_count: int = 0
    first_event_at: str = ""
    last_event_at: str = ""
    doc_session_seconds: float = 0.0
    counts: dict[str, int] = field(default_factory=lambda: defaultdict(int))


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

    query = (
        "SELECT user_id, assignment_id, group_id, file_path, event_type, timestamp, data "
        f"FROM {sql_identifier(table)} "
        "ORDER BY timestamp"
    )
    with psycopg.connect(**connect_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            for (
                user_id,
                assignment_id,
                group_id,
                file_path,
                event_type,
                timestamp,
                data,
            ) in cursor:
                yield {
                    "user_id": user_id,
                    "assignment_id": assignment_id,
                    "group_id": group_id,
                    "file_path": file_path,
                    "event_type": event_type,
                    "timestamp": timestamp,
                    "data": as_data(data),
                }


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
            event = json.loads(line)
        except json.JSONDecodeError as err:
            raise SystemExit(f"psql output line {line_number}: invalid JSON: {err}") from err
        event["data"] = as_data(event.get("data"))
        yield event


def find_psql() -> str | None:
    psql_path = shutil.which("psql")
    if psql_path is not None:
        return psql_path

    program_files = Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
    candidates = sorted(program_files.glob(r"PostgreSQL/*/bin/psql.exe"), reverse=True)
    return str(candidates[0]) if candidates else None


def psql_json_query(table: str) -> str:
    return (
        "SELECT json_build_object("
        "'user_id', user_id, "
        "'assignment_id', assignment_id, "
        "'group_id', group_id, "
        "'file_path', file_path, "
        "'event_type', event_type, "
        "'timestamp', timestamp, "
        "'data', data"
        ")::text "
        f"FROM {sql_identifier(table)} "
        "ORDER BY timestamp"
    )


def key_for_event(event: dict[str, Any]) -> tuple[str, str, str, str, str, str, str]:
    data = event["data"]
    return (
        str(event.get("user_id") or ""),
        str(event.get("assignment_id") or ""),
        str(event.get("group_id") or ""),
        str(data.get("session_id") or ""),
        str(data.get("condition") or ""),
        str(data.get("course_id") or ""),
        str(data.get("task_id") or ""),
    )


def update_row(row: MetricRow, event: dict[str, Any]) -> None:
    event_type = str(event.get("event_type") or "")
    data = event["data"]
    row.event_count += 1
    if event_type in EVENT_FIELDS:
        row.counts[event_type] += 1
    if event_type == "doc_session":
        duration = data.get("duration_seconds")
        if isinstance(duration, (int, float)):
            row.doc_session_seconds += float(duration)

    parsed_timestamp = parse_timestamp(event.get("timestamp"))
    if parsed_timestamp is not None:
        timestamp_text = parsed_timestamp.isoformat()
        if not row.first_event_at or timestamp_text < row.first_event_at:
            row.first_event_at = timestamp_text
        if not row.last_event_at or timestamp_text > row.last_event_at:
            row.last_event_at = timestamp_text


def export_metrics(events: Iterable[dict[str, Any]], output_path: Path) -> None:
    rows: dict[tuple[str, str, str, str, str, str, str], MetricRow] = {}
    for event in events:
        key = key_for_event(event)
        row = rows.setdefault(key, MetricRow(*key))
        update_row(row, event)

    fieldnames = [
        "user_id",
        "assignment_id",
        "group_id",
        "session_id",
        "condition",
        "course_id",
        "task_id",
        "event_count",
        "first_event_at",
        "last_event_at",
        "doc_session_seconds",
        *[f"{event_type}_events" for event_type in EVENT_FIELDS],
    ]
    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in sorted(rows.values(), key=lambda r: (r.user_id, r.session_id)):
            writer.writerow(
                {
                    "user_id": row.user_id,
                    "assignment_id": row.assignment_id,
                    "group_id": row.group_id,
                    "session_id": row.session_id,
                    "condition": row.condition,
                    "course_id": row.course_id,
                    "task_id": row.task_id,
                    "event_count": row.event_count,
                    "first_event_at": row.first_event_at,
                    "last_event_at": row.last_event_at,
                    "doc_session_seconds": f"{row.doc_session_seconds:.3f}",
                    **{
                        f"{event_type}_events": row.counts[event_type]
                        for event_type in EVENT_FIELDS
                    },
                }
            )


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
        help="Output CSV file. Defaults to a timestamped capture-metrics-YYYYMMDD-HHMMSS.csv file.",
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
    output_path = args.out or default_output_path()
    export_metrics(events, output_path)
    print(f"Wrote {output_path}")


def default_output_path() -> Path:
    timestamp = datetime.now(timezone.utc).astimezone().strftime("%Y%m%d-%H%M%S")
    return Path(f"capture-metrics-{timestamp}.csv")


if __name__ == "__main__":
    main()
