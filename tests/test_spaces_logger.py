from __future__ import annotations

import gzip
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.config import Config, SpaceSource, load_config
from app.db import Database, GIB
from app.main import create_app
from app.parsers import AccessEvent, parse_access_log, truncate_ip
from app.services import AlertEngine, BillingSync, _usage_bytes


ORIGIN = (
    '79a bucket [06/Feb/2019:00:00:38 +0000] 192.0.2.14 - req-origin '
    'REST.GET.OBJECT photos%2Fcat.jpg "GET /bucket/photos/cat.jpg HTTP/1.1" '
    '200 - 123 456 9 8 "https://example.com/" "curl/8.0" - host SigV4 TLS auth bucket TLS_AES - -'
)

CLOUDFRONT = """#Version: 1.0
#Fields: date time x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie) x-edge-result-type x-edge-request-id x-host-header cs-protocol cs-bytes time-taken
2019-12-04\t21:02:31\tFRA2\t456\t2001:db8::1\tGET\td.example\t/photos/dog.jpg\t200\t-\tcurl%2F8\t-\t-\tHit\treq-cdn\tcdn.example\thttps\t20\t0.025
bad\trow
"""


def make_config(path: Path, **overrides) -> Config:
    values = dict(
        sources=(SpaceSource(region="fra1", log_bucket="logs", log_prefix="access-logs/", source_bucket="bucket"),),
        database_path=path,
        timezone="Asia/Baku",
        request_retention_days=30,
        rollup_retention_days=396,
        sync_minutes=15,
        spaces_key=None,
        spaces_secret=None,
        spaces_profile=None,
        digitalocean_token=None,
        slack_webhook_url=None,
    )
    values.update(overrides)
    return Config(**values)


def test_public_log_bucket_config_shape(tmp_path: Path, monkeypatch):
    path = tmp_path / "config.toml"
    path.write_text(
        '[[logs]]\nregion="tor1"\nbucket="example-log-bucket"\nprefix="access-logs/"\n'
    )
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    config = load_config(path)
    assert config.sources == (
        SpaceSource(region="tor1", log_bucket="example-log-bucket", log_prefix="access-logs/"),
    )


def test_origin_and_gzipped_cdn_parsing():
    origin, rejected = parse_access_log(ORIGIN.encode(), "different-fallback")
    assert rejected == 0
    assert origin[0].bucket == "bucket"
    assert origin[0].bytes_out == 123
    assert origin[0].object_key == "photos/cat.jpg"
    assert origin[0].client_network == "192.0.2.0/24"

    cdn, rejected = parse_access_log(gzip.compress(CLOUDFRONT.encode()), "bucket")
    assert len(cdn) == 1 and rejected == 1
    assert cdn[0].source == "cdn"
    assert cdn[0].cache_result == "Hit"
    assert cdn[0].latency_ms == 25
    assert cdn[0].client_network == "2001:db8::/48"


def test_ip_truncation_rejects_invalid_values():
    assert truncate_ip("203.0.113.45") == "203.0.113.0/24"
    assert truncate_ip("not-an-ip") is None


def test_ingestion_is_idempotent_and_rolls_back(tmp_path: Path):
    db = Database(tmp_path / "db.sqlite")
    db.initialize()
    events, _ = parse_access_log(ORIGIN.encode(), "bucket")
    assert db.ingest_object("logs", "one", "etag-1", events, 0) == 1
    assert db.ingest_object("logs", "two", "etag-2", events, 0) == 0
    assert db.query("SELECT SUM(bytes_out) value FROM hourly_usage")[0]["value"] == 123

    broken = AccessEvent(**{**events[0].__dict__, "request_id": "new", "source": "invalid"})
    with pytest.raises(Exception):
        db.ingest_object("logs", "broken", "etag-3", [broken], 0)
    assert not db.object_processed("logs", "broken", "etag-3")


def test_retention(tmp_path: Path):
    db = Database(tmp_path / "db.sqlite")
    db.initialize()
    old = (datetime.now(timezone.utc) - timedelta(days=40)).isoformat()
    event = AccessEvent("bucket", old, "origin", "old", "x", "GET", "GET", 200, None, 1, None, None, None, None)
    db.ingest_object("logs", "old", "etag", [event], 0)
    removed, _ = db.cleanup(30, 396)
    assert removed == 1


def test_billing_quantity_and_sync(tmp_path: Path):
    assert _usage_bytes({"duration": "1.5", "duration_unit": "GiB"}) == int(1.5 * GIB)
    config = make_config(tmp_path / "db.sqlite", digitalocean_token="token")
    db = Database(config.database_path)
    db.initialize()
    response = {
        "invoice_items": [{
            "product": "Spaces Bandwidth", "description": "bucket transfer",
            "duration": "2", "duration_unit": "GiB", "amount": "0.02",
        }],
        "links": {},
    }
    with patch("app.services._request_json", return_value=response):
        result = BillingSync(config, db).run()
    assert result.records == 1
    row = db.query("SELECT * FROM billing_records")[0]
    assert row["bucket"] == "bucket" and row["usage_bytes"] == 2 * GIB


def test_alert_deduplication(tmp_path: Path):
    config = make_config(tmp_path / "db.sqlite", slack_webhook_url="https://invalid.example")
    db = Database(config.database_path)
    db.initialize()
    db.execute("INSERT INTO thresholds(name,limit_bytes) VALUES('tiny',1)")
    event = AccessEvent("bucket", datetime.now(timezone.utc).isoformat(), "origin", "new", "x", "GET", "GET", 200, None, 2, None, None, None, None)
    db.ingest_object("logs", "new", "etag", [event], 0)
    sent = []
    engine = AlertEngine(config, db)
    engine._send = sent.append
    engine.evaluate()
    engine.evaluate()
    assert len(sent) == 1


def test_dashboard_and_threshold_api(tmp_path: Path):
    config = make_config(tmp_path / "db.sqlite", sources=())
    database = Database(config.database_path)
    database.initialize()
    database.set_progress(
        "access", "running", datetime.now(timezone.utc).isoformat(),
        objects_total=100, objects_scanned=25, objects_imported=20, objects_skipped=5, requests_imported=100,
        current_key="access-logs/example.gz",
    )
    with TestClient(create_app(config)) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.get("/dashboard").status_code == 200
        assert client.get("/requests").status_code == 200
        assert client.get("/settings").status_code == 200
        progress = client.get("/api/sync/progress").json()[0]
        assert progress["status"] == "running" and progress["objects_scanned"] == 25
        assert progress["objects_total"] == 100
        assert client.get("/api/summary?period=90d").status_code == 200
        assert client.get("/api/summary?period=13m").status_code == 200
        created = client.post("/api/thresholds", json={"name": "Bucket cap", "bucket": "bucket", "limit_gib": 3})
        assert created.status_code == 201
        threshold_id = created.json()["id"]
        assert client.delete(f"/api/thresholds/{threshold_id}").status_code == 204
        assert client.get("/api/summary?period=wrong").status_code == 400
        assert client.post(
            "/api/thresholds",
            headers={"origin": "https://evil.example", "host": "dashboard.example"},
            json={"name": "Bad", "limit_gib": 1},
        ).status_code == 403
