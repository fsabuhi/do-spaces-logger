from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import boto3
from botocore.config import Config as BotoConfig

from .config import Config, SpaceSource
from .db import Database, GIB
from .parsers import parse_access_log


log = logging.getLogger(__name__)


@dataclass
class SyncResult:
    kind: str
    scanned: int = 0
    objects: int = 0
    skipped: int = 0
    requests: int = 0
    rejected: int = 0
    records: int = 0


class AccessLogSync:
    def __init__(self, config: Config, database: Database):
        self.config = config
        self.database = database

    def _client(self, source: SpaceSource):
        if bool(self.config.spaces_key) != bool(self.config.spaces_secret):
            raise RuntimeError("Both Spaces access-key variables must be configured")
        credentials = (
            {"aws_access_key_id": self.config.spaces_key, "aws_secret_access_key": self.config.spaces_secret}
            if self.config.spaces_key else {}
        )
        session = boto3.Session(profile_name=self.config.spaces_profile if not credentials else None)
        if not credentials and session.get_credentials() is None:
            raise RuntimeError("No Spaces credentials or AWS profile are configured")
        return session.client(
            "s3",
            region_name=source.region,
            endpoint_url=f"https://{source.region}.digitaloceanspaces.com",
            config=BotoConfig(retries={"max_attempts": 4, "mode": "standard"}),
            **credentials,
        )

    def run(self) -> SyncResult:
        started = datetime.now(timezone.utc).isoformat()
        result = SyncResult(kind="access")
        current_key = ""
        objects: list[tuple[SpaceSource, Any, dict[str, Any]]] = []
        self.database.set_progress("access", "running", started, message="Counting log objects")

        def progress(status: str = "running", message: str = "") -> None:
            self.database.set_progress(
                "access", status, started,
                objects_total=len(objects),
                objects_scanned=result.scanned,
                objects_imported=result.objects,
                objects_skipped=result.skipped,
                requests_imported=result.requests,
                rejected_rows=result.rejected,
                current_key=current_key,
                message=message,
            )

        try:
            if not self.config.sources:
                raise RuntimeError("No Spaces sources are configured")
            for source in self.config.sources:
                client = self._client(source)
                paginator = client.get_paginator("list_objects_v2")
                for page in paginator.paginate(Bucket=source.log_bucket, Prefix=source.log_prefix):
                    for item in page.get("Contents", []):
                        objects.append((source, client, item))
                    self.database.set_progress(
                        "access", "running", started, objects_total=len(objects),
                        message=f"Counting log objects: {len(objects):,} found",
                    )
            progress(message="Processing log objects")
            for source, client, item in objects:
                current_key = item["Key"]
                result.scanned += 1
                etag = item.get("ETag", "").strip('"')
                if self.database.object_processed(source.log_bucket, current_key, etag):
                    result.skipped += 1
                    if result.scanned % 25 == 0:
                        progress()
                    continue
                payload = client.get_object(Bucket=source.log_bucket, Key=current_key)["Body"].read()
                events, rejected = parse_access_log(payload, source.source_bucket)
                if rejected and not events:
                    raise ValueError(f"{source.log_bucket}/{current_key}: no valid log rows")
                result.requests += self.database.ingest_object(
                    source.log_bucket, current_key, etag, events, rejected
                )
                result.objects += 1
                result.rejected += rejected
                if result.scanned % 25 == 0:
                    progress()
            self.database.record_sync("access", started, "ok", json.dumps(asdict(result)))
            progress("ok", "Sync complete")
            return result
        except Exception as error:
            self.database.record_sync("access", started, "error", str(error))
            progress("error", str(error))
            raise


def _request_json(url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def _usage_bytes(item: dict[str, Any]) -> int | None:
    unit = str(item.get("duration_unit", "")).lower().replace(" ", "")
    multipliers = {
        "bytes": 1,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "tb": 1000**4,
        "kib": 1024,
        "mib": 1024**2,
        "gib": GIB,
        "tib": 1024**4,
        "gigabyte": 1000**3,
        "gigabytes": 1000**3,
        "gibibyte": GIB,
        "gibibytes": GIB,
    }
    if unit not in multipliers:
        return None
    try:
        return int(Decimal(str(item["duration"])) * multipliers[unit])
    except (InvalidOperation, KeyError, TypeError, ValueError):
        return None


class BillingSync:
    def __init__(self, config: Config, database: Database):
        self.config = config
        self.database = database

    def run(self) -> SyncResult:
        started = datetime.now(timezone.utc).isoformat()
        result = SyncResult(kind="billing")
        self.database.set_progress("billing", "running", started, message="Fetching invoice preview")
        try:
            if not self.config.digitalocean_token:
                raise RuntimeError("DigitalOcean billing token is not configured")
            page = 1
            items: list[dict[str, Any]] = []
            while True:
                payload = _request_json(
                    f"https://api.digitalocean.com/v2/customers/my/invoices/preview?per_page=200&page={page}",
                    self.config.digitalocean_token,
                )
                items.extend(payload.get("invoice_items", []))
                next_url = payload.get("links", {}).get("pages", {}).get("next")
                if not next_url:
                    break
                page += 1
            now = datetime.now(timezone.utc)
            source_names = [source.source_bucket for source in self.config.sources if source.source_bucket]
            source_names.extend(
                row["bucket"] for row in self.database.query("SELECT DISTINCT bucket FROM request_events")
                if row["bucket"] not in source_names
            )
            with self.database.transaction() as connection:
                for item in items:
                    searchable = " ".join(
                        str(item.get(field, "")) for field in ("product", "description", "group_description")
                    )
                    lowered = searchable.lower()
                    if "space" not in lowered or not any(word in lowered for word in ("bandwidth", "transfer")):
                        continue
                    bucket = next((name for name in source_names if name.lower() in lowered), "")
                    connection.execute(
                        """INSERT OR REPLACE INTO billing_records
                        (observed_at,period,bucket,description,sku,usage_bytes,amount_usd)
                        VALUES(?,?,?,?,?,?,?)""",
                        (
                            now.isoformat(), now.strftime("%Y-%m"), bucket,
                            str(item.get("description") or item.get("product") or "Spaces bandwidth"),
                            str(item.get("sku") or item.get("resource_id") or ""),
                            _usage_bytes(item), str(item.get("amount", "0")),
                        ),
                    )
                    result.records += 1
            self.database.record_sync("billing", started, "ok", json.dumps(asdict(result)))
            self.database.set_progress(
                "billing", "ok", started, requests_imported=result.records, message="Sync complete"
            )
            return result
        except Exception as error:
            self.database.record_sync("billing", started, "error", str(error))
            self.database.set_progress("billing", "error", started, message=str(error))
            raise


class AlertEngine:
    def __init__(self, config: Config, database: Database):
        self.config = config
        self.database = database

    def _send(self, message: str) -> None:
        if not self.config.slack_webhook_url:
            raise RuntimeError("Slack webhook is not configured")
        body = json.dumps({"text": message}).encode()
        request = urllib.request.Request(
            self.config.slack_webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            if response.status >= 300:
                raise RuntimeError(f"Slack returned HTTP {response.status}")

    def _fire(self, kind: str, key: str, value: float, message: str) -> bool:
        if self.database.query("SELECT 1 FROM alert_events WHERE dedupe_key=?", (key,)):
            return False
        self._send(message)
        try:
            self.database.execute(
                "INSERT INTO alert_events(kind,dedupe_key,fired_at,value,message) VALUES(?,?,?,?,?)",
                (kind, key, datetime.now(timezone.utc).isoformat(), value, message),
            )
        except Exception:
            # Another concurrent evaluator may have recorded the same alert.
            return False
        return True

    def evaluate(self) -> list[str]:
        if not self.config.slack_webhook_url:
            return []
        fired: list[str] = []
        now = datetime.now(timezone.utc)
        month = now.strftime("%Y-%m")
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).isoformat()
        for threshold in self.database.query("SELECT * FROM thresholds WHERE enabled=1 ORDER BY limit_bytes"):
            bucket_sql = "AND bucket=?" if threshold["bucket"] else ""
            params: tuple[Any, ...] = (month_start, threshold["bucket"]) if threshold["bucket"] else (month_start,)
            usage = self.database.query(
                f"SELECT COALESCE(SUM(bytes_out),0) total FROM hourly_usage WHERE hour >= ? {bucket_sql}", params
            )[0]["total"]
            if usage >= threshold["limit_bytes"]:
                label = threshold["bucket"] or "all Spaces buckets"
                message = f"Spaces bandwidth alert: {label} used {usage / GIB:.2f} GiB ({threshold['name']})."
                if self._fire("usage", f"threshold:{threshold['id']}:{month}", usage, message):
                    fired.append(message)

        recent = self.database.query(
            "SELECT status FROM sync_runs WHERE kind='access' ORDER BY id DESC LIMIT 4"
        )
        if len(recent) == 4 and all(row["status"] == "error" for row in recent):
            message = "Spaces logger health alert: the last four access-log syncs failed."
            key = f"sync:{now.strftime('%Y-%m-%d-%H')}"
            if self._fire("health", key, 4, message):
                fired.append(message)

        window = (now - timedelta(minutes=15)).isoformat()
        stats = self.database.query(
            """SELECT COUNT(*) total, SUM(CASE WHEN status BETWEEN 500 AND 599 THEN 1 ELSE 0 END) errors
            FROM request_events WHERE occurred_at >= ?""",
            (window,),
        )[0]
        total, errors = stats["total"], stats["errors"] or 0
        if total >= 100 and errors / total > 0.05:
            slot = now.replace(minute=0, second=0, microsecond=0).isoformat()
            message = f"Spaces 5xx alert: {errors}/{total} requests ({errors / total:.1%}) failed in 15 minutes."
            if self._fire("5xx", f"5xx:{slot}", errors / total, message):
                fired.append(message)
        return fired

    def send_test(self) -> None:
        self._send("Spaces logger test alert: Slack delivery is working.")
