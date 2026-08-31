# DigitalOcean Spaces Access Log Dashboard

A self-hosted dashboard for DigitalOcean Spaces origin and CDN access logs.
It imports logs directly from a Spaces bucket, tracks bandwidth and request
activity, reconciles usage with the DigitalOcean Billing API, and sends Slack
alerts.

## What you get

- Origin and CDN log ingestion without downloading logs manually
- Live first-sync percentage with safe resume after restarts
- Bandwidth, requests, 4xx/5xx responses, CDN cache hits, and top objects
- Request explorer with 30-day detail retention and truncated client IPs
- Hourly analytics retained for 13 months
- Optional Billing API reconciliation and Slack alerts
- SQLite storage and a Docker deployment protected by HTTPS and login

## How it works

```text
Source Space(s) ──access logs──▶ Log Space ──read-only──▶ Dashboard
                                                        ├── SQLite
DigitalOcean Billing API ──────optional─────────────────┤
Slack webhook ◀────────────────optional alerts──────────┘
```

The application reads the **log destination bucket**, not the objects in your
source bucket. Origin log rows identify their source bucket automatically.
DigitalOcean requires the source and log destination to be different buckets
in the same region.

## Prerequisites

- Python 3.11+ for a direct installation, or Docker with Docker Compose
- A DigitalOcean Spaces access key that can read the log destination bucket
- The destination bucket's exact region, bucket name, and log prefix
- For a public Docker deployment, a DNS name pointing to the server

The Billing API token and Slack webhook are optional.

## Quick start with existing logs

If this already works:

```sh
aws --profile your-profile s3 ls s3://your-log-bucket/access-logs/
```

you are almost ready.

### 1. Install

```sh
git clone <repository-url>
cd do_spaces_logger
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### 2. Create the real configuration

```sh
cp config.example.toml config.toml
cp .env.example .env
```

Edit `config.toml`:

```toml
timezone = "UTC"
sync_minutes = 15
request_retention_days = 30
rollup_retention_days = 396

[[logs]]
region = "fra1"
bucket = "your-log-bucket"
prefix = "access-logs/"
```

Important:

- `region` is the actual Spaces endpoint region, such as `fra1`, `nyc3`,
  or `sgp1). It is not necessarily the name of your AWS CLI profile.
- `bucket` is the bucket containing log files.
- `prefix` is the folder-like key prefix inside that bucket.
- Add another `[[logs]]` block for each destination bucket, region, or
  separately configured prefix.

### 3. Configure credentials

For a direct local run, reuse a working AWS CLI profile by editing `.env`:

```dotenv
AWS_PROFILE=your-profile
```

Alternatively, use explicit read-only Spaces credentials:

```dotenv
SPACES_ACCESS_KEY_ID=your-access-key
SPACES_SECRET_ACCESS_KEY=your-secret-key
```

Do not set both methods. Never commit `.env); it is already ignored by Git.

### 4. Run

```sh
.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000 --env-file .env
```

Open <http://127.0.0.1:8000>.

The first run counts every object under the configured prefix, then displays
an exact completion percentage. Previously processed objects are shown as
`skipped`; this is expected and prevents double-counting after a restart.

## Set up access logs from scratch

Skip this section if log objects already exist.

### 1. Create a destination bucket

Create a standard Spaces bucket dedicated to logs:

- It must be different from the source bucket.
- It must be in the same region as the source bucket.
- One regional destination can receive logs from multiple source buckets.
- Use a distinct prefix per source bucket when practical.

Example:

```text
Source bucket:      assets-production
Region:             fra1
Log bucket:         company-spaces-logs-fra1
Destination prefix: access-logs/assets-production/
```

### 2. Enable logging on the source bucket

Create `logging.json`:

```json
{
  "LoggingEnabled": {
    "TargetBucket": "company-spaces-logs-fra1",
    "TargetPrefix": "access-logs/assets-production/"
  }
}
```

Apply it with a full-access Spaces key used only for setup:

```sh
aws --profile your-setup-profile \
  --endpoint-url https://fra1.digitaloceanspaces.com \
  s3api put-bucket-logging \
  --bucket assets-production \
  --bucket-logging-status file://logging.json
```

Verify the configuration:

```sh
aws --profile your-setup-profile \
  --endpoint-url https://fra1.digitaloceanspaces.com \
  s3api get-bucket-logging \
  --bucket assets-production
```

Access-log delivery is asynchronous. It commonly takes about an hour and can
take two hours or longer. Generate some traffic, then verify that objects
appear:

```sh
aws --profile your-runtime-profile \
  --endpoint-url https://fra1.digitaloceanspaces.com \
  s3 ls s3://company-spaces-logs-fra1/access-logs/assets-production/
```

After setup, do not run the dashboard with the full-access setup key. Create a
separate runtime key with read access to the log bucket.

### 3. Configure retention for raw logs

The dashboard never deletes objects from Spaces. Configure a lifecycle rule on
the log bucket if you do not want raw logs retained forever. Ninety days is a
reasonable starting point; choose a period that matches your audit and privacy
requirements.

## CDN and multiple buckets

Origin log rows contain the source bucket name, so several origin buckets can
share one configured log prefix.

CDN logs normally include a Spaces hostname from which the application derives
a bucket label. If a CDN uses a custom hostname, configure its prefix
separately and provide the source bucket explicitly:

```toml
[[logs]]
region = "fra1"
bucket = "company-spaces-logs-fra1"
prefix = "access-logs/assets-production/"
source_bucket = "assets-production"
```

For multiple regions, add one block per regional log destination:

```toml
[[logs]]
region = "fra1"
bucket = "company-logs-fra1"
prefix = "access-logs/"

[[logs]]
region = "nyc3"
bucket = "company-logs-nyc3"
prefix = "access-logs/"
```

## Optional Billing API reconciliation

Create a DigitalOcean API token restricted to `billing:read`, then set:

```dotenv
DIGITALOCEAN_TOKEN=your-read-only-token
```

The application checks the current invoice preview once daily. When a Spaces
bandwidth line item includes a usable quantity, the dashboard compares it with
the access-log estimate. These totals can differ because billing excludes some
private traffic and uses different accounting boundaries.

Billing failure does not stop access-log ingestion.

## Optional Slack alerts

Create a Slack incoming webhook and set:

```dotenv
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

Use Settings or `POST /api/alerts/test` to test delivery. Default alerts:

- 80% and 100% of the shared 1,024 GiB monthly Spaces allowance
- Four consecutive access-log sync failures
- More than 5% 5xx responses over 15 minutes, with at least 100 requests

Additional account-wide or per-bucket bandwidth thresholds can be created in
Settings.

## Docker deployment with HTTPS and login

The included Compose deployment runs one application worker and Caddy. Only
Caddy publishes ports; the FastAPI container is private to the Compose network.

### 1. Configure application secrets

For Docker, use explicit Spaces credentials in `.env), not an AWS profile:

```dotenv
SPACES_ACCESS_KEY_ID=your-read-only-key
SPACES_SECRET_ACCESS_KEY=your-read-only-secret
```

Set a public DNS name:

```dotenv
DOMAIN=spaces-logs.example.com
ADMIN_USER=admin
```

Generate a password hash:

```sh
docker run --rm -i caddy:2.10-alpine caddy hash-password
```

Put the result in single quotes so Compose preserves the `$` characters:

```dotenv
ADMIN_PASSWORD_HASH='$2a$14$your_generated_hash'
```

### 2. Start

```sh
docker compose up -d --build
docker compose logs -f app caddy
```

Allow inbound TCP 80/443 and UDP 443. Caddy obtains and renews the TLS
certificate automatically. Back up the `spaces-data` volume, which contains
the SQLite database and alert settings.

## Data retention and privacy

- Individual request rows are retained for 30 days by default.
- Client IPv4 addresses are truncated to `/24`; IPv6 addresses to `/48`.
- Hourly analytics are retained for 396 days by default.
- Raw log objects remain in Spaces until their lifecycle policy removes them.
- The application only lists and reads configured log bucket prefixes.
- Imported objects are identified by log bucket, object key, and ETag.

Change local retention in `config.toml`:

```toml
request_retention_days = 30
rollup_retention_days = 396
```

## Sync behavior

- Access logs are checked every `sync_minutes`.
- The first run lists the complete prefix to calculate an exact percentage.
- `scanned` includes both new and previously processed objects.
- `imported` means a new log object was parsed; an imported object may contain
  no request rows.
- `skipped` means the same key and ETag were already processed.
- `requests` counts newly inserted, deduplicated request rows in the current
  sync.
- A restart safely resumes by scanning and skipping completed objects.

The Overview page supports 24-hour, 7-day, 30-day, 90-day, 13-month, and
current-month ranges.

## Troubleshooting

### `NoSuchBucket`

Confirm that `logs.bucket` is the destination bucket name and `logs.region`
is its actual Spaces region. A profile called `do-tor1`, for example, does
not prove that the bucket endpoint is `tor1).

Test the exact endpoint:

```sh
aws --profile your-profile \
  --endpoint-url https://your-region.digitaloceanspaces.com \
  s3 ls s3://your-log-bucket/your-prefix/
```

### `No Spaces credentials or AWS profile are configured`

For direct runs, set `AWS_PROFILE` in `.env). For Docker, set both explicit
Spaces key variables and ensure the key can read the log bucket.

### Sync imports objects but shows zero requests

Spaces may create CDN batches containing only headers and no request rows.
Watch the current key: the scan can process many `spaces-cdn-*` objects before
reaching `spaces-origin-*` objects. Empty objects are still recorded so they
are not downloaded again.

### Recent traffic is missing

Spaces delivers logs asynchronously, commonly after an hour and sometimes
after two hours or more. Recent dashboard totals are provisional.

### Login is missing locally

The direct development server intentionally has no built-in login. Caddy
provides HTTPS and authentication in the Docker deployment. Do not expose the
direct Uvicorn port publicly.

## API

Deployment routes are protected by Caddy authentication.

- `GET /api/summary?period=24h|7d|30d|90d|13m|month`
- `GET /api/requests?bucket=&source=&status=&object_prefix=&cursor=&limit=`
- `GET /api/sync/progress`
- `POST /api/sync/access`
- `POST /api/sync/billing`
- `GET /api/thresholds`
- `POST /api/thresholds`
- `DELETE /api/thresholds/{id}`
- `POST /api/alerts/test`
- `GET /metrics`
- `GET /healthz`
- `GET /readyz`

FastAPI also serves the generated schema at `/openapi.json` and interactive docs
at `/docs`.

### Prometheus

`GET /metrics` returns the Prometheus text exposition format, so Grafana,
Alertmanager, Datadog, and anything else that scrapes can build their own
dashboards and alert rules on top of the collected usage. It sits behind the
same Caddy basic auth as the rest of the site:

```yaml
scrape_configs:
  - job_name: spaces
    scrape_interval: 60s
    scheme: https
    basic_auth:
      username: admin
      password: your-admin-password
    static_configs:
      - targets: ["spaces.example.com"]
```

Exported gauges:

| Metric | Labels | Meaning |
| --- | --- | --- |
| `spaces_month_bytes_out` | `bucket`, `source` | Bytes served month-to-date |
| `spaces_month_requests` | `bucket`, `source` | Requests served month-to-date |
| `spaces_threshold_limit_bytes` | `name`, `bucket` | Configured threshold; empty bucket means all buckets |
| `spaces_recent_requests` | `status_class` | Requests in the last 15 minutes |
| `spaces_cdn_cache_hit_ratio` | | CDN cache hit ratio month-to-date |
| `spaces_billing_usage_bytes` | `period` | Bytes reported by the Billing API |
| `spaces_billing_amount_usd` | `period` | Bandwidth charges reported by the Billing API |
| `spaces_reconciliation_delta_bytes` | | Billed bytes minus measured bytes |
| `spaces_sync_success_timestamp_seconds` | `kind` | Unix time of the last successful sync |
| `spaces_sync_status` | `kind` | 1 if the most recent sync succeeded, 0 if it failed |

A value with no observation yet is reported as `NaN`, which Prometheus treats as
absent. Each scrape runs a handful of aggregate queries, so keep the interval at
30s or slower.

Two rules to start with:

```yaml
groups:
  - name: spaces
    rules:
      - alert: SpacesSyncStale
        expr: time() - spaces_sync_success_timestamp_seconds{kind="access"} > 3600
        annotations:
          summary: "No successful access-log sync in over an hour"

      - alert: SpacesBandwidthBudget
        expr: |
          sum(spaces_month_bytes_out)
            / min(spaces_threshold_limit_bytes{bucket=""}) > 0.8
        annotations:
          summary: "Spaces bandwidth is over 80% of the monthly allowance"
```

## Development

```sh
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest
.venv/bin/uvicorn app.main:app --reload --env-file .env
```

The test suite covers origin/CDN parsing, gzip input, IP truncation,
idempotency, transaction rollback, retention, Billing API conversion, alert
deduplication, configuration loading, dashboard routes, sync progress, and
same-origin mutation checks.

## Official documentation

- [Configure DigitalOcean Spaces access logs](https://docs.digitalocean.com/products/spaces/how-to/access-logs/)
- [DigitalOcean Spaces limits](https://docs.digitalocean.com/products/spaces/details/limits/)
- [DigitalOcean Billing API](https://docs.digitalocean.com/platform/billing/reference/api/)
- [DigitalOcean bandwidth billing](https://docs.digitalocean.com/platform/billing/bandwidth/)
