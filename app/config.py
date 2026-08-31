from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SpaceSource:
    region: str
    log_bucket: str
    log_prefix: str
    source_bucket: str | None = None


@dataclass(frozen=True)
class Config:
    sources: tuple[SpaceSource, ...]
    database_path: Path
    timezone: str
    request_retention_days: int
    rollup_retention_days: int
    sync_minutes: int
    spaces_key: str | None
    spaces_secret: str | None
    spaces_profile: str | None
    digitalocean_token: str | None
    slack_webhook_url: str | None


def load_config(path: str | Path | None = None) -> Config:
    config_path = Path(path or os.getenv("APP_CONFIG", "config.toml"))
    raw = tomllib.loads(config_path.read_text()) if config_path.exists() else {}
    log_items = raw.get("logs", [])
    # Keep accepting the first-draft shape so an early local config does not break.
    legacy_items = raw.get("spaces", []) if not log_items else []
    sources = tuple(
        SpaceSource(
            region=item["region"],
            log_bucket=item["bucket"],
            log_prefix=item.get("prefix", "access-logs/"),
            source_bucket=item.get("source_bucket"),
        )
        for item in log_items
    ) + tuple(
        SpaceSource(
            region=item["region"],
            log_bucket=item["log_bucket"],
            log_prefix=item.get("log_prefix", f"access-logs/{item['name']}/"),
            source_bucket=item["name"],
        )
        for item in legacy_items
    )
    return Config(
        sources=sources,
        database_path=Path(os.getenv("DATABASE_PATH", raw.get("database_path", "data/spaces.db"))),
        timezone=os.getenv("APP_TIMEZONE", raw.get("timezone", "Asia/Baku")),
        request_retention_days=int(raw.get("request_retention_days", 30)),
        rollup_retention_days=int(raw.get("rollup_retention_days", 396)),
        sync_minutes=int(raw.get("sync_minutes", 15)),
        spaces_key=os.getenv("SPACES_ACCESS_KEY_ID"),
        spaces_secret=os.getenv("SPACES_SECRET_ACCESS_KEY"),
        spaces_profile=os.getenv("AWS_PROFILE") or raw.get("aws_profile"),
        digitalocean_token=os.getenv("DIGITALOCEAN_TOKEN"),
        slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
    )
