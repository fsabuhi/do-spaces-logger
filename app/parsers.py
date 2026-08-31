from __future__ import annotations

import gzip
import ipaddress
import shlex
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import unquote


@dataclass(frozen=True)
class AccessEvent:
    bucket: str
    occurred_at: str
    source: str
    request_id: str
    object_key: str
    operation: str
    method: str
    status: int
    error_code: str | None
    bytes_out: int
    latency_ms: float | None
    cache_result: str | None
    client_network: str | None
    user_agent: str | None


def truncate_ip(value: str) -> str | None:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return None
    prefix = 24 if address.version == 4 else 48
    return str(ipaddress.ip_network(f"{address}/{prefix}", strict=False))


def _integer(value: str) -> int:
    return 0 if value in {"", "-"} else int(value)


def _optional(value: str) -> str | None:
    return None if value in {"", "-"} else value


def _origin_tokens(line: str) -> list[str]:
    start, end = line.find("["), line.find("]")
    if start < 0 or end < start:
        raise ValueError("missing S3 timestamp")
    quoted_time = line[:start] + '"' + line[start + 1 : end] + '"' + line[end + 1 :]
    return shlex.split(quoted_time)


def parse_origin_line(line: str, fallback_bucket: str | None = None) -> AccessEvent:
    fields = _origin_tokens(line)
    if len(fields) < 17:
        raise ValueError(f"expected at least 17 S3 fields, got {len(fields)}")
    occurred = datetime.strptime(fields[2], "%d/%b/%Y:%H:%M:%S %z").astimezone(timezone.utc)
    request_parts = fields[8].split()
    method = request_parts[0] if request_parts else fields[6].split(".")[1]
    return AccessEvent(
        bucket=fields[1] if fields[1] != "-" else (fallback_bucket or "unknown"),
        occurred_at=occurred.isoformat(),
        source="origin",
        request_id=fields[5],
        object_key=unquote("" if fields[7] == "-" else fields[7]),
        operation=fields[6],
        method=method,
        status=_integer(fields[9]),
        error_code=_optional(fields[10]),
        bytes_out=_integer(fields[11]),
        latency_ms=float(fields[13]) if fields[13] != "-" else None,
        cache_result=None,
        client_network=truncate_ip(fields[3]),
        user_agent=_optional(fields[16]),
    )


def _cdn_bucket(item: dict[str, str], fallback_bucket: str | None) -> str:
    if fallback_bucket:
        return fallback_bucket
    host = item.get("x-host-header") or item.get("cs(Host)") or "unknown"
    if host.endswith(".digitaloceanspaces.com"):
        return host.split(".", 1)[0]
    return host


def parse_access_log(payload: bytes, fallback_bucket: str | None = None) -> tuple[list[AccessEvent], int]:
    if payload.startswith(b"\x1f\x8b"):
        payload = gzip.decompress(payload)
    lines = payload.decode("utf-8", errors="replace").splitlines()
    cloudfront_fields: list[str] | None = None
    events: list[AccessEvent] = []
    rejected = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("#Fields:"):
            cloudfront_fields = line.removeprefix("#Fields:").strip().split()
            continue
        if line.startswith("#"):
            continue
        try:
            if cloudfront_fields:
                values = line.split("\t")
                if len(values) != len(cloudfront_fields):
                    raise ValueError("CloudFront field count mismatch")
                item = dict(zip(cloudfront_fields, values, strict=True))
                occurred = datetime.fromisoformat(f"{item['date']}T{item['time']}+00:00")
                events.append(
                    AccessEvent(
                        bucket=_cdn_bucket(item, fallback_bucket),
                        occurred_at=occurred.isoformat(),
                        source="cdn",
                        request_id=item["x-edge-request-id"],
                        object_key=unquote(item.get("cs-uri-stem", "").lstrip("/")),
                        operation=item.get("cs-method", "-"),
                        method=item.get("cs-method", "-"),
                        status=_integer(item.get("sc-status", "0")),
                        error_code=None,
                        bytes_out=_integer(item.get("sc-bytes", "0")),
                        latency_ms=float(item["time-taken"]) * 1000 if item.get("time-taken") not in {None, "-"} else None,
                        cache_result=_optional(item.get("x-edge-result-type", "-")),
                        client_network=truncate_ip(item.get("c-ip", "-")),
                        user_agent=_optional(item.get("cs(User-Agent)", "-")),
                    )
                )
            else:
                events.append(parse_origin_line(line, fallback_bucket))
        except (KeyError, TypeError, ValueError):
            rejected += 1
    return events, rejected
