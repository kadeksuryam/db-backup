# dbbackup

Database backup tool with pluggable engines, storage backends, and GFS retention policies.

Currently supports **PostgreSQL** with S3-compatible and SSH storage. Designed to easily add new database engines (MySQL, MongoDB, etc.).

## Features

- **Multi-engine architecture** — pluggable database backends via the `engines/` package
- **Multiple storage backends** — S3-compatible (AWS S3, Cloudflare R2, MinIO) and SSH/scp
- **GFS retention** — Grandfather-Father-Son pruning (keep_last, daily, weekly, monthly, yearly)
- **Multi-version PostgreSQL** — use different `pg_dump`/`psql` versions per datasource
- **Secret management** — resolve credentials from environment variables using `*_env` keys
- **Restore with safety checks** — warns before overwriting existing databases

## Project Structure

```
dbbackup/
├── dbbackup.py            # CLI entrypoint
├── config.py              # YAML config loading and validation
├── backup.py              # Backup orchestrator
├── restore.py             # Restore orchestrator
├── retention.py           # GFS retention algorithm
├── engines/
│   ├── __init__.py        # Engine ABC + factory
│   └── postgres.py        # PostgreSQL engine (pg_dump/psql)
├── stores/
│   ├── __init__.py        # Store ABC + factory + parse_timestamp
│   ├── s3.py              # S3-compatible storage (boto3)
│   └── ssh.py             # SSH/scp storage
├── tests/                 # Unit tests (223 tests)
├── config.example.yaml    # Example configuration
├── Dockerfile             # Production image
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

The config file has three sections:

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
```

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

### Jobs

Link datasources to stores with a prefix and optional retention policy:

```yaml
jobs:
  appdb-backup:
    datasource: appdb
    store: r2
    prefix: prod
    retention:
      keep_last: 3
      keep_daily: 7
      keep_weekly: 4
      keep_monthly: 6
      keep_yearly: 1
```

Backup files are stored at: `<prefix>/<database>/<database>-<YYYYMMDD-HHMMSS>.sql.gz`

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

# Run all jobs
docker run --rm -v ... -e ... dbbackup backup --all

# Backup and prune in one step
docker run --rm -v ... -e ... dbbackup backup appdb-backup --prune
docker run --rm -v ... -e ... dbbackup backup --all --prune
```

### List Backups

```bash
docker run --rm -v ... -e ... dbbackup list appdb-backup
```

### Restore

```bash
# Restore the latest backup
docker run --rm -v ... -e ... dbbackup restore appdb-backup

# Restore a specific backup file
docker run --rm -v ... -e ... dbbackup restore appdb-backup db-20260210-143000.sql.gz

# Skip confirmation prompt (for automation)
docker run --rm -v ... -e ... dbbackup restore appdb-backup --auto-confirm
```

### Prune (Apply Retention)

```bash
docker run --rm -v ... -e ... dbbackup prune appdb-backup
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

## Development

```bash
# Requires Python 3.11+
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pytest

# Run all tests
python -m pytest tests/ -v

# Run a specific test file
python -m pytest tests/test_retention.py -v
```

## Adding a New Engine

1. Create `engines/<name>.py` implementing the `Engine` ABC
2. Add the engine to `_ENGINE_TYPES` in `engines/__init__.py`
3. Add `create()` factory function to the new module
4. Update the Dockerfile if the engine needs additional client tools
