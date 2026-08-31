from __future__ import annotations

import sqlite3
import threading
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator

from .parsers import AccessEvent


GIB = 1024**3

SCHEMA = """
CREATE TABLE IF NOT EXISTS request_events (
    id INTEGER PRIMARY KEY,
    bucket TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    source TEXT NOT NULL CHECK(source IN ('origin', 'cdn')),
    request_id TEXT NOT NULL,
    object_key TEXT NOT NULL,
    operation TEXT NOT NULL,
    method TEXT NOT NULL,
    status INTEGER NOT NULL,
    error_code TEXT,
    bytes_out INTEGER NOT NULL,
    latency_ms REAL,
    cache_result TEXT,
    client_network TEXT,
    user_agent TEXT,
    UNIQUE(bucket, source, request_id)
);
CREATE INDEX IF NOT EXISTS request_time_idx ON request_events(occurred_at);
CREATE INDEX IF NOT EXISTS request_filter_idx ON request_events(bucket, source, status, occurred_at);

CREATE TABLE IF NOT EXISTS hourly_usage (
    hour TEXT NOT NULL,
    bucket TEXT NOT NULL,
    source TEXT NOT NULL,
    object_key TEXT NOT NULL,
    operation TEXT NOT NULL,
    status_class INTEGER NOT NULL,
    cache_result TEXT NOT NULL,
    requests INTEGER NOT NULL,
    bytes_out INTEGER NOT NULL,
    total_latency_ms REAL NOT NULL,
    PRIMARY KEY(hour, bucket, source, object_key, operation, status_class, cache_result)
);
CREATE INDEX IF NOT EXISTS hourly_time_idx ON hourly_usage(hour);

CREATE TABLE IF NOT EXISTS processed_objects (
    log_bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    etag TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    parsed_rows INTEGER NOT NULL,
    rejected_rows INTEGER NOT NULL,
    PRIMARY KEY(log_bucket, object_key, etag)
);

CREATE TABLE IF NOT EXISTS sync_runs (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    status TEXT NOT NULL,
    detail TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_progress (
    kind TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    objects_total INTEGER NOT NULL DEFAULT 0,
    objects_scanned INTEGER NOT NULL DEFAULT 0,
    objects_imported INTEGER NOT NULL DEFAULT 0,
    objects_skipped INTEGER NOT NULL DEFAULT 0,
    requests_imported INTEGER NOT NULL DEFAULT 0,
    rejected_rows INTEGER NOT NULL DEFAULT 0,
    current_key TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS billing_records (
    observed_at TEXT NOT NULL,
    period TEXT NOT NULL,
    bucket TEXT NOT NULL,
    description TEXT NOT NULL,
    sku TEXT NOT NULL,
    usage_bytes INTEGER,
    amount_usd TEXT NOT NULL,
    PRIMARY KEY(observed_at, bucket, description, sku)
);

CREATE TABLE IF NOT EXISTS thresholds (
    id INTEGER PRIMARY KEY,
    bucket TEXT,
    name TEXT NOT NULL,
    limit_bytes INTEGER NOT NULL CHECK(limit_bytes > 0),
    enabled INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS alert_events (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    fired_at TEXT NOT NULL,
    value REAL NOT NULL,
    message TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(SCHEMA)
            progress_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(sync_progress)")
            }
            if "objects_total" not in progress_columns:
                connection.execute(
                    "ALTER TABLE sync_progress ADD COLUMN objects_total INTEGER NOT NULL DEFAULT 0"
                )
            existing = connection.execute("SELECT COUNT(*) FROM thresholds").fetchone()[0]
            if not existing:
                connection.executemany(
                    "INSERT INTO thresholds(bucket, name, limit_bytes) VALUES(NULL, ?, ?)",
                    (("Shared allowance 80%", int(1024 * GIB * 0.8)), ("Shared allowance 100%", 1024 * GIB)),
                )

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock, self.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
            except Exception:
                connection.rollback()
                raise
            else:
                connection.commit()

    def object_processed(self, log_bucket: str, key: str, etag: str) -> bool:
        with self.connect() as connection:
            return connection.execute(
                "SELECT 1 FROM processed_objects WHERE log_bucket=? AND object_key=? AND etag=?",
                (log_bucket, key, etag),
            ).fetchone() is not None

    def ingest_object(self, log_bucket: str, key: str, etag: str, events: Iterable[AccessEvent], rejected: int) -> int:
        rollups: dict[tuple, list[float]] = defaultdict(lambda: [0, 0, 0.0])
        inserted = 0
        with self.transaction() as connection:
            for event in events:
                cursor = connection.execute(
                    """INSERT INTO request_events
                    (bucket, occurred_at, source, request_id, object_key, operation, method, status,
                     error_code, bytes_out, latency_ms, cache_result, client_network, user_agent)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(bucket, source, request_id) DO NOTHING""",
                    (
                        event.bucket, event.occurred_at, event.source, event.request_id, event.object_key,
                        event.operation, event.method, event.status, event.error_code, event.bytes_out,
                        event.latency_ms, event.cache_result, event.client_network, event.user_agent,
                    ),
                )
                if not cursor.rowcount:
                    continue
                inserted += 1
                hour = event.occurred_at[:13] + ":00:00+00:00"
                group = (
                    hour, event.bucket, event.source, event.object_key, event.operation,
                    event.status // 100, event.cache_result or "",
                )
                rollups[group][0] += 1
                rollups[group][1] += event.bytes_out
                rollups[group][2] += event.latency_ms or 0
            connection.executemany(
                """INSERT INTO hourly_usage
                (hour,bucket,source,object_key,operation,status_class,cache_result,requests,bytes_out,total_latency_ms)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(hour,bucket,source,object_key,operation,status_class,cache_result)
                DO UPDATE SET requests=requests+excluded.requests,
                              bytes_out=bytes_out+excluded.bytes_out,
                              total_latency_ms=total_latency_ms+excluded.total_latency_ms""",
                ((*group, int(values[0]), int(values[1]), values[2]) for group, values in rollups.items()),
            )
            connection.execute(
                "INSERT OR IGNORE INTO processed_objects VALUES(?,?,?,?,?,?)",
                (log_bucket, key, etag, datetime.now(timezone.utc).isoformat(), inserted, rejected),
            )
        return inserted

    def record_sync(self, kind: str, started_at: str, status: str, detail: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self.connect() as connection:
            connection.execute(
                "INSERT INTO sync_runs(kind,started_at,finished_at,status,detail) VALUES(?,?,?,?,?)",
                (kind, started_at, now, status, detail[:1000]),
            )

    def set_progress(
        self,
        kind: str,
        status: str,
        started_at: str,
        *,
        objects_total: int = 0,
        objects_scanned: int = 0,
        objects_imported: int = 0,
        objects_skipped: int = 0,
        requests_imported: int = 0,
        rejected_rows: int = 0,
        current_key: str = "",
        message: str = "",
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._write_lock, self.connect() as connection:
            connection.execute(
                """INSERT INTO sync_progress
                (kind,status,started_at,updated_at,objects_total,objects_scanned,objects_imported,objects_skipped,
                 requests_imported,rejected_rows,current_key,message)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(kind) DO UPDATE SET
                  status=excluded.status, started_at=excluded.started_at, updated_at=excluded.updated_at,
                  objects_total=excluded.objects_total,
                  objects_scanned=excluded.objects_scanned, objects_imported=excluded.objects_imported,
                  objects_skipped=excluded.objects_skipped, requests_imported=excluded.requests_imported,
                  rejected_rows=excluded.rejected_rows, current_key=excluded.current_key,
                  message=excluded.message""",
                (
                    kind, status, started_at, now, objects_total, objects_scanned, objects_imported, objects_skipped,
                    requests_imported, rejected_rows, current_key, message[:1000],
                ),
            )

    def cleanup(self, request_days: int, rollup_days: int) -> tuple[int, int]:
        now = datetime.now(timezone.utc)
        with self.transaction() as connection:
            requests = connection.execute(
                "DELETE FROM request_events WHERE occurred_at < ?",
                ((now - timedelta(days=request_days)).isoformat(),),
            ).rowcount
            rollups = connection.execute(
                "DELETE FROM hourly_usage WHERE hour < ?",
                ((now - timedelta(days=rollup_days)).isoformat(),),
            ).rowcount
        return requests, rollups

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self.connect() as connection:
            return list(connection.execute(sql, params))

    def execute(self, sql: str, params: tuple = ()) -> int:
        with self._write_lock, self.connect() as connection:
            cursor = connection.execute(sql, params)
            return cursor.lastrowid if sql.lstrip().upper().startswith("INSERT") else cursor.rowcount
