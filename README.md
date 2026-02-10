# dbbackup

Database backup tool with pluggable engines, storage backends, and GFS retention policies.

Currently supports **PostgreSQL** with S3-compatible and SSH storage. Designed to easily add new database engines (MySQL, MongoDB, etc.).

## Features

- **Multi-engine architecture** — pluggable database backends via the `engines/` package
- **Multiple storage backends** — S3-compatible (AWS S3, Cloudflare R2, MinIO) and SSH/scp
- **GFS retention** — Grandfather-Father-Son pruning (keep_last, daily, weekly, monthly, yearly) with dry-run support
- **Multi-version PostgreSQL** — use different `pg_dump`/`psql` versions per datasource
- **Secret management** — resolve credentials from environment variables using `*_env` keys
- **Restore with safety checks** — integrity verification and user confirmation before overwriting databases
- **Backup verification** — optional post-backup download and integrity check
- **Upload integrity** — S3 uploads verified by comparing local/remote file sizes
- **SHA256 checksums** — sidecar `.sha256` files uploaded alongside backups, verified on restore
- **Retry with backoff** — configurable retry attempts with exponential backoff per job
- **Email notifications** — notify on success, failure, or always; supports multiple recipients
- **Parallel execution** — run multiple backup jobs concurrently with `--parallel N`
- **Dry-run mode** — preview what prune would delete without actually deleting
- **Summary logging** — per-job timing and success/failure counts for multi-job runs
- **Security** — minimal environment for subprocesses, shell-escaped SSH commands, file permissions checks

## Project Structure

```
dbbackup/
├── dbbackup.py            # CLI entrypoint
├── config.py              # YAML config loading and validation
├── backup.py              # Backup orchestrator
├── restore.py             # Restore orchestrator
├── retention.py           # GFS retention algorithm
├── utils.py               # Shared utilities (sha256, format_size)
├── engines/
│   ├── __init__.py        # Engine ABC + factory
│   └── postgres.py        # PostgreSQL engine (pg_dump/psql)
├── stores/
│   ├── __init__.py        # Store ABC + factory + parse_timestamp
│   ├── s3.py              # S3-compatible storage (boto3)
│   └── ssh.py             # SSH/scp storage
├── notifiers/
│   ├── __init__.py        # Notifier ABC + factory
│   └── email.py           # Email notification (SMTP)
├── tests/                 # Unit tests (390+ tests)
├── config.example.yaml    # Example configuration
├── Dockerfile             # Production image
├── .dockerignore          # Excludes tests, docs, etc. from image
└── requirements.txt       # Python dependencies
```

## Setup

```bash
# Build with PostgreSQL 17 client (default)
docker build -t dbbackup .

# Build with multiple PostgreSQL client versions
docker build --build-arg PG_VERSIONS="14 17" -t dbbackup .
```

## Configuration

Copy and edit the example config:

```bash
cp config.example.yaml config.yaml
```

The config file has four sections: **datasources**, **stores**, **notifications** (optional), and **jobs**.

### Datasources

Define database connections. Each datasource requires an `engine` field:

```yaml
datasources:
  appdb:
    engine: postgres
    host: db.example.com
    port: 5432
    user: postgres
    password_env: APPDB_PASSWORD    # reads $APPDB_PASSWORD at runtime
    database: appdb
    pg_version: 17                  # optional: use versioned pg_dump binary
    timeout: 3600                   # optional: subprocess timeout in seconds
```

#### Dump Format & Compression

Each datasource supports optional `format`, `compression`, and `compression_level` options:

| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `format` | `plain`, `custom` | `plain` | `plain` = SQL text (restored with `psql`); `custom` = binary (restored with `pg_restore`) |
| `compression` | `gzip`, `zstd`, `lz4`, `none` | `gzip` | External compressor piped after `pg_dump` |
| `compression_level` | integer (1-19) | tool default | Passed as level flag to the compressor (gzip=6, zstd=3, lz4=1) |

Backup file extensions reflect the chosen format and compression:

| format \ compression | gzip | zstd | lz4 | none |
|---|---|---|---|---|
| **plain** | `.sql.gz` | `.sql.zst` | `.sql.lz4` | `.sql` |
| **custom** | `.dump.gz` | `.dump.zst` | `.dump.lz4` | `.dump` |

Example using binary format with zstd compression:

```yaml
datasources:
  warehousedb:
    engine: postgres
    host: warehouse-db.internal
    port: 5432
    user: warehouse
    password_env: WAREHOUSE_PASSWORD
    database: warehouse
    format: custom
    compression: zstd
    compression_level: 5
```

Restore automatically detects the format and compression from the file extension, so backups made with any combination can be restored regardless of the current datasource settings.

### Stores

Define backup storage destinations:

```yaml
stores:
  # S3-compatible (AWS S3, Cloudflare R2, MinIO)
  r2:
    type: s3
    endpoint: https://your-account-id.r2.cloudflarestorage.com
    bucket: db-backups
    access_key_env: R2_ACCESS_KEY
    secret_key_env: R2_SECRET_KEY

  # SSH/scp remote host
  backup-server:
    type: ssh
    host: backup.example.com
    user: backup
    port: 22
    path: /data/db-backups
    key_file: /keys/id_ed25519     # optional
```

### Notifications

Define notification backends (optional). Currently supports email via SMTP:

```yaml
notifications:
  email_ops:
    type: email
    smtp_host: smtp.example.com
    smtp_port: 587
    username_env: SMTP_USER
    password_env: SMTP_PASS
    from: backups@example.com
    to:                               # single address or list
      - ops@example.com
      - dev-team@example.com
    use_tls: true
```

The `to` field accepts a single email address (string), a comma-separated string, or a YAML list.

### Jobs

Link datasources to stores with a prefix and optional settings:

```yaml
jobs:
  appdb-backup:
    datasource: appdb
    store: r2
    prefix: prod
    verify: true                     # download + verify after upload
    retry:
      max_attempts: 3                # total attempts (1 = no retry, default)
      delay: 30                      # seconds before first retry
      backoff_multiplier: 2          # exponential backoff multiplier
    notify:
      - notifier: email_ops
        on: failure                  # "failure", "success", or "always"
    retention:
      keep_last: 3
      keep_daily: 7
      keep_weekly: 4
      keep_monthly: 6
      keep_yearly: 1
```

Backup files are stored at: `<prefix>/<database>/<database>-<YYYYMMDD-HHMMSS>.<ext>`

## Usage

All commands run via Docker. The entrypoint is `dbbackup.py`, so pass commands directly after the image name.

```bash
# Base command — mount config and pass secrets via env vars
docker run --rm \
  -v /path/to/config.yaml:/config/config.yaml \
  -e APPDB_PASSWORD=secret \
  -e R2_ACCESS_KEY=key \
  -e R2_SECRET_KEY=secret \
  dbbackup <command>
```

### Backup

```bash
# Run a single job
docker run --rm -v ... -e ... dbbackup backup appdb-backup

# Run all jobs sequentially
docker run --rm -v ... -e ... dbbackup backup --all

# Run all jobs with up to 4 in parallel
docker run --rm -v ... -e ... dbbackup backup --all --parallel 4

# Backup and prune in one step
docker run --rm -v ... -e ... dbbackup backup appdb-backup --prune

# Backup, prune, run all in parallel
docker run --rm -v ... -e ... dbbackup backup --all --prune --parallel 4
```

When running multiple jobs, a summary is logged at the end showing per-job timings and overall success/failure counts.

### List Backups

```bash
docker run --rm -v ... -e ... dbbackup list appdb-backup
```

Output includes timestamp, human-readable size, and remote key for each backup.

### Restore

```bash
# Restore the latest backup
docker run --rm -v ... -e ... dbbackup restore appdb-backup

# Restore a specific backup file
docker run --rm -v ... -e ... dbbackup restore appdb-backup db-20260210-143000.sql.gz

# Skip confirmation prompt (for automation)
docker run --rm -v ... -e ... dbbackup restore appdb-backup --auto-confirm
```

The restore process:
1. Downloads the backup file
2. Verifies backup integrity (file structure check)
3. Verifies SHA256 checksum if a `.sha256` sidecar exists
4. Checks for existing tables and prompts before dropping
5. Restores the backup

### Prune (Apply Retention)

```bash
# Apply retention policy, deleting expired backups
docker run --rm -v ... -e ... dbbackup prune appdb-backup

# Preview what would be deleted (dry-run)
docker run --rm -v ... -e ... dbbackup prune appdb-backup --dry-run
```

### Custom Config Path

```bash
# Mount config to a custom path and pass it via env var
docker run --rm \
  -v /path/to/config.yaml:/custom/config.yaml \
  -e DBBACKUP_CONFIG=/custom/config.yaml \
  -e APPDB_PASSWORD=secret \
  dbbackup backup --all
```

Or use `-c` / `--config`:

```bash
docker run --rm \
  -v /path/to/config.yaml:/custom/config.yaml \
  dbbackup -c /custom/config.yaml backup --all
```

## Retention Policy

Uses the GFS (Grandfather-Father-Son) model, same as Proxmox Backup Server and sanoid:

| Rule | Description |
|------|-------------|
| `keep_last` | Always keep the N most recent backups |
| `keep_daily` | Keep the newest backup per day, for the last N days |
| `keep_weekly` | Keep the newest backup per ISO week, for the last N weeks |
| `keep_monthly` | Keep the newest backup per month, for the last N months |
| `keep_yearly` | Keep the newest backup per year, for the last N years |

Rules are combined with union logic — a backup kept by **any** rule is protected from deletion. If no retention rules are configured for a job, all backups are kept.

When deleting backups, associated `.sha256` sidecar files are also cleaned up.

## Development

```bash
# Requires Python 3.11+
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pytest

# Run all tests (390+ tests)
python -m pytest tests/ -v

# Run a specific test file
python -m pytest tests/test_retention.py -v
```

## Adding a New Engine

1. Create `engines/<name>.py` implementing the `Engine` ABC
2. Add the engine to `_ENGINE_TYPES` in `engines/__init__.py`
3. Add `create()` factory function to the new module
4. Update the Dockerfile if the engine needs additional client tools

## Adding a New Notifier

1. Create `notifiers/<name>.py` implementing the `Notifier` ABC
2. Add the notifier to `_NOTIFIER_TYPES` in `notifiers/__init__.py`
3. Add `create(config: dict)` factory function to the new module

## Adding a New Store

1. Create `stores/<name>.py` implementing the `Store` ABC
2. Add the store to `_STORE_TYPES` in `stores/__init__.py`
3. Add `create(config: dict)` factory function to the new module
