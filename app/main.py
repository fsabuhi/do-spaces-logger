from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from fastapi import BackgroundTasks, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from .config import Config, load_config
from .db import Database, GIB
from .services import AccessLogSync, AlertEngine, BillingSync


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(__name__)
APP_DIR = Path(__file__).parent


class ThresholdInput(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    bucket: str | None = Field(default=None, max_length=255)
    limit_gib: float = Field(gt=0)


def _start_for(period: str) -> datetime:
    now = datetime.now(timezone.utc)
    if period == "24h":
        return now - timedelta(hours=24)
    if period == "7d":
        return now - timedelta(days=7)
    if period == "30d":
        return now - timedelta(days=30)
    if period == "90d":
        return now - timedelta(days=90)
    if period == "13m":
        return now - timedelta(days=396)
    if period == "month":
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    raise HTTPException(400, "period must be 24h, 7d, 30d, 90d, 13m, or month")


def _same_origin(request: Request) -> None:
    origin = request.headers.get("origin")
    if origin and urlparse(origin).netloc != request.headers.get("host"):
        raise HTTPException(403, "cross-origin mutation rejected")


def _summary(database: Database, period: str) -> dict:
    start = _start_for(period).isoformat()
    totals = database.query(
        """SELECT COALESCE(SUM(bytes_out),0) bytes_out, COALESCE(SUM(requests),0) requests,
        COALESCE(SUM(CASE WHEN status_class=4 THEN requests ELSE 0 END),0) errors_4xx,
        COALESCE(SUM(CASE WHEN status_class=5 THEN requests ELSE 0 END),0) errors_5xx,
        COALESCE(SUM(CASE WHEN source='cdn' AND cache_result IN ('Hit','RefreshHit') THEN requests ELSE 0 END),0) cache_hits,
        COALESCE(SUM(CASE WHEN source='cdn' THEN requests ELSE 0 END),0) cdn_requests
        FROM hourly_usage WHERE hour >= ?""",
        (start,),
    )[0]
    buckets = database.query(
        """SELECT bucket, SUM(bytes_out) bytes_out, SUM(requests) requests,
        SUM(CASE WHEN source='origin' THEN bytes_out ELSE 0 END) origin_bytes,
        SUM(CASE WHEN source='cdn' THEN bytes_out ELSE 0 END) cdn_bytes
        FROM hourly_usage WHERE hour >= ? GROUP BY bucket ORDER BY bytes_out DESC""",
        (start,),
    )
    series = database.query(
        "SELECT hour, SUM(bytes_out) bytes_out, SUM(requests) requests FROM hourly_usage WHERE hour >= ? GROUP BY hour ORDER BY hour",
        (start,),
    )
    top_objects = database.query(
        """SELECT bucket, object_key, SUM(bytes_out) bytes_out, SUM(requests) requests
        FROM hourly_usage WHERE hour >= ? AND object_key != ''
        GROUP BY bucket, object_key ORDER BY bytes_out DESC LIMIT 20""",
        (start,),
    )
    billing = database.query(
        """SELECT COALESCE(SUM(usage_bytes),0) usage_bytes, COALESCE(SUM(CAST(amount_usd AS REAL)),0) amount_usd,
        MAX(observed_at) observed_at FROM billing_records
        WHERE period=? AND observed_at=(SELECT MAX(observed_at) FROM billing_records WHERE period=?)""",
        (datetime.now(timezone.utc).strftime("%Y-%m"),) * 2,
    )[0]
    syncs = database.query(
        """SELECT s.* FROM sync_runs s JOIN (
        SELECT kind, MAX(id) id FROM sync_runs GROUP BY kind
        ) latest ON latest.id=s.id ORDER BY kind"""
    )
    progress = database.query("SELECT * FROM sync_progress ORDER BY kind")
    data = dict(totals)
    data["cache_hit_rate"] = data["cache_hits"] / data["cdn_requests"] if data["cdn_requests"] else None
    data.update(
        period=period,
        start=start,
        buckets=[dict(row) for row in buckets],
        series=[dict(row) for row in series],
        top_objects=[dict(row) for row in top_objects],
        billing=dict(billing),
        reconciliation_delta=(billing["usage_bytes"] - data["bytes_out"]) if billing["observed_at"] else None,
        syncs=[dict(row) for row in syncs],
        progress=[dict(row) for row in progress],
    )
    return data


def _escape(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _labels(**pairs: object) -> str:
    return "{" + ",".join(f'{key}="{_escape(value)}"' for key, value in pairs.items()) + "}" if pairs else ""


def _metrics(database: Database) -> str:
    """Render the Prometheus text exposition format so external tooling can scrape usage."""
    # ponytail: one SQL pass per scrape, no cache - add caching if the scrape interval drops below 30s.
    now = datetime.now(timezone.utc)
    month = _start_for("month").isoformat()
    lines: list[str] = []

    def family(name: str, help_text: str, samples: list[tuple[str, float | None]]) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")
        lines.extend(f"{name}{labels} {'NaN' if value is None else value}" for labels, value in samples)

    usage = database.query(
        """SELECT bucket, source, SUM(bytes_out) bytes_out, SUM(requests) requests
        FROM hourly_usage WHERE hour >= ? GROUP BY bucket, source""",
        (month,),
    )
    family(
        "spaces_month_bytes_out",
        "Bytes served month-to-date.",
        [(_labels(bucket=row["bucket"], source=row["source"]), row["bytes_out"]) for row in usage],
    )
    family(
        "spaces_month_requests",
        "Requests served month-to-date.",
        [(_labels(bucket=row["bucket"], source=row["source"]), row["requests"]) for row in usage],
    )

    family(
        "spaces_threshold_limit_bytes",
        "Configured monthly bandwidth threshold; an empty bucket label means all buckets.",
        [
            (_labels(name=row["name"], bucket=row["bucket"] or ""), row["limit_bytes"])
            for row in database.query("SELECT name, bucket, limit_bytes FROM thresholds WHERE enabled=1")
        ],
    )

    recent = database.query(
        "SELECT status/100 status_class, COUNT(*) requests FROM request_events WHERE occurred_at >= ? GROUP BY 1",
        ((now - timedelta(minutes=15)).isoformat(),),
    )
    family(
        "spaces_recent_requests",
        "Requests in the last 15 minutes, by status class.",
        [(_labels(status_class=row["status_class"]), row["requests"]) for row in recent],
    )

    cache = database.query(
        """SELECT COALESCE(SUM(CASE WHEN cache_result IN ('Hit','RefreshHit') THEN requests ELSE 0 END),0) hits,
        COALESCE(SUM(requests),0) total FROM hourly_usage WHERE hour >= ? AND source='cdn'""",
        (month,),
    )[0]
    family(
        "spaces_cdn_cache_hit_ratio",
        "CDN cache hit ratio month-to-date.",
        [("", cache["hits"] / cache["total"] if cache["total"] else None)],
    )

    period = now.strftime("%Y-%m")
    billing = database.query(
        """SELECT COALESCE(SUM(usage_bytes),0) usage_bytes, COALESCE(SUM(CAST(amount_usd AS REAL)),0) amount_usd,
        MAX(observed_at) observed_at FROM billing_records
        WHERE period=? AND observed_at=(SELECT MAX(observed_at) FROM billing_records WHERE period=?)""",
        (period,) * 2,
    )[0]
    billed = billing["usage_bytes"] if billing["observed_at"] else None
    family(
        "spaces_billing_usage_bytes",
        "Bandwidth bytes reported by the DigitalOcean Billing API.",
        [(_labels(period=period), billed)],
    )
    family(
        "spaces_billing_amount_usd",
        "Spaces bandwidth charges reported by the DigitalOcean Billing API.",
        [(_labels(period=period), billing["amount_usd"] if billing["observed_at"] else None)],
    )
    family(
        "spaces_reconciliation_delta_bytes",
        "Billed bytes minus measured bytes month-to-date.",
        [("", billed - sum(row["bytes_out"] for row in usage) if billed is not None else None)],
    )

    family(
        "spaces_sync_success_timestamp_seconds",
        "Unix time of the last successful sync of each kind.",
        [
            (_labels(kind=row["kind"]), datetime.fromisoformat(row["finished_at"]).timestamp())
            for row in database.query(
                """SELECT s.kind, s.finished_at FROM sync_runs s JOIN (
                SELECT kind, MAX(id) id FROM sync_runs WHERE status='ok' GROUP BY kind
                ) latest ON latest.id=s.id"""
            )
        ],
    )
    family(
        "spaces_sync_status",
        "1 if the most recent sync of this kind succeeded, 0 if it failed.",
        [
            (_labels(kind=row["kind"]), int(row["status"] == "ok"))
            for row in database.query(
                """SELECT s.kind, s.status FROM sync_runs s JOIN (
                SELECT kind, MAX(id) id FROM sync_runs GROUP BY kind
                ) latest ON latest.id=s.id"""
            )
        ],
    )
    return "\n".join(lines) + "\n"


def create_app(config: Config | None = None) -> FastAPI:
    settings = config or load_config()
    database = Database(settings.database_path)
    database.initialize()
    access_sync = AccessLogSync(settings, database)
    billing_sync = BillingSync(settings, database)
    alerts = AlertEngine(settings, database)
    sync_lock = asyncio.Lock()
    stop = asyncio.Event()

    async def run_sync(kind: Literal["access", "billing"]):
        async with sync_lock:
            service = access_sync if kind == "access" else billing_sync
            result = await asyncio.to_thread(service.run)
            await asyncio.to_thread(alerts.evaluate)
            return result

    async def scheduler() -> None:
        billing_day = ""
        cleanup_day = ""
        while not stop.is_set():
            now = datetime.now(timezone.utc)
            if settings.sources:
                try:
                    await run_sync("access")
                except Exception:
                    log.exception("scheduled access-log sync failed")
                    with suppress(Exception):
                        await asyncio.to_thread(alerts.evaluate)
            if settings.digitalocean_token and billing_day != now.date().isoformat():
                try:
                    await run_sync("billing")
                    billing_day = now.date().isoformat()
                except Exception:
                    log.exception("scheduled billing sync failed")
            if cleanup_day != now.date().isoformat():
                try:
                    await asyncio.to_thread(
                        database.cleanup, settings.request_retention_days, settings.rollup_retention_days
                    )
                    cleanup_day = now.date().isoformat()
                except Exception:
                    log.exception("scheduled retention cleanup failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=settings.sync_minutes * 60)
            except TimeoutError:
                pass

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        task = asyncio.create_task(scheduler())
        yield
        stop.set()
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    app = FastAPI(title="Spaces Access Log Dashboard", version="1.0.0", lifespan=lifespan)
    app.state.config = settings
    app.state.db = database
    app.state.run_sync = run_sync
    templates = Jinja2Templates(directory=APP_DIR / "templates")
    templates.env.filters["gib"] = lambda value: f"{(value or 0) / GIB:,.2f} GiB"
    templates.env.filters["localtime"] = lambda value: (
        datetime.fromisoformat(value).astimezone(ZoneInfo(settings.timezone)).strftime("%Y-%m-%d %H:%M %Z")
        if value else "Never"
    )
    app.mount("/static", StaticFiles(directory=APP_DIR / "static"), name="static")

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz():
        database.query("SELECT 1")
        return {"status": "ready"}

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics():
        return PlainTextResponse(_metrics(database), media_type="text/plain; version=0.0.4; charset=utf-8")

    @app.get("/api/summary")
    def api_summary(period: str = "month"):
        return _summary(database, period)

    @app.get("/api/sync/progress")
    def sync_progress():
        return [dict(row) for row in database.query("SELECT * FROM sync_progress ORDER BY kind")]

    @app.get("/api/requests")
    def api_requests(
        bucket: str | None = None,
        source: Literal["origin", "cdn"] | None = None,
        status: int | None = Query(default=None, ge=100, le=599),
        object_prefix: str | None = None,
        cursor: int | None = None,
        limit: int = Query(default=100, ge=1, le=500),
    ):
        clauses, params = ["1=1"], []
        for column, value in (("bucket", bucket), ("source", source), ("status", status)):
            if value is not None:
                clauses.append(f"{column}=?")
                params.append(value)
        if object_prefix:
            clauses.append("object_key LIKE ? ESCAPE '\\'")
            params.append(object_prefix.replace("%", "\\%").replace("_", "\\_") + "%")
        if cursor:
            clauses.append("id < ?")
            params.append(cursor)
        params.append(limit)
        rows = database.query(
            f"SELECT * FROM request_events WHERE {' AND '.join(clauses)} ORDER BY id DESC LIMIT ?", tuple(params)
        )
        return {"items": [dict(row) for row in rows], "next_cursor": rows[-1]["id"] if len(rows) == limit else None}

    @app.get("/api/thresholds")
    def list_thresholds():
        return [dict(row) for row in database.query("SELECT * FROM thresholds ORDER BY limit_bytes")]

    @app.post("/api/thresholds", status_code=201)
    def create_threshold(request: Request, threshold: ThresholdInput):
        _same_origin(request)
        threshold_id = database.execute(
            "INSERT INTO thresholds(bucket,name,limit_bytes) VALUES(?,?,?)",
            (threshold.bucket or None, threshold.name, int(threshold.limit_gib * GIB)),
        )
        return {"id": threshold_id}

    @app.delete("/api/thresholds/{threshold_id}", status_code=204)
    def delete_threshold(request: Request, threshold_id: int):
        _same_origin(request)
        if not database.execute("DELETE FROM thresholds WHERE id=?", (threshold_id,)):
            raise HTTPException(404, "threshold not found")

    @app.post("/api/sync/{kind}")
    async def sync_now(request: Request, kind: Literal["access", "billing"]):
        _same_origin(request)
        try:
            return await run_sync(kind)
        except RuntimeError as error:
            raise HTTPException(503, str(error)) from error

    @app.post("/api/alerts/test", status_code=204)
    async def test_alert(request: Request):
        _same_origin(request)
        try:
            await asyncio.to_thread(alerts.send_test)
        except RuntimeError as error:
            raise HTTPException(503, str(error)) from error

    @app.get("/", response_class=HTMLResponse)
    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard(request: Request, period: str = "month"):
        return templates.TemplateResponse(request, "dashboard.html", {"summary": _summary(database, period)})

    @app.get("/requests", response_class=HTMLResponse)
    def request_page(request: Request, bucket: str | None = None, source: str | None = None):
        rows = api_requests(
            bucket=bucket, source=source or None, status=None, object_prefix=None, cursor=None, limit=200
        )
        return templates.TemplateResponse(
            request, "requests.html", {"result": rows, "bucket": bucket or "", "source": source or ""}
        )

    @app.get("/settings", response_class=HTMLResponse)
    def settings_page(request: Request):
        return templates.TemplateResponse(
            request,
            "settings.html",
            {
                "thresholds": list_thresholds(),
                "sources": settings.sources,
                "slack_enabled": bool(settings.slack_webhook_url),
            },
        )

    @app.post("/settings/thresholds")
    def settings_add_threshold(
        request: Request,
        name: Annotated[str, Form()],
        limit_gib: Annotated[float, Form(gt=0)],
        bucket: Annotated[str, Form()] = "",
    ):
        _same_origin(request)
        create_threshold(request, ThresholdInput(name=name, bucket=bucket or None, limit_gib=limit_gib))
        return RedirectResponse("/settings", status_code=303)

    @app.post("/settings/thresholds/{threshold_id}/delete")
    def settings_delete_threshold(request: Request, threshold_id: int):
        _same_origin(request)
        delete_threshold(request, threshold_id)
        return RedirectResponse("/settings", status_code=303)

    return app


app = create_app()
