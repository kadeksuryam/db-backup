"""Configuration loading, validation, and env-var resolution."""

from __future__ import annotations

import logging
import os
import stat
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml

log = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "/config/config.yaml"

# Standard datasource keys (everything else goes into options).
_DS_STANDARD_KEYS = {"engine", "host", "port", "user", "password", "database"}


@dataclass
class Datasource:
    name: str
    engine: str  # "postgres", "mysql", etc.
    host: str
    port: int
    user: str
    password: str
    database: str
    options: dict = field(default_factory=dict)  # engine-specific settings


def build_prefix(prefix: str, dbname: str) -> str:
    """Build the storage prefix path: <prefix>/<dbname>."""
    parts = [p for p in [prefix, dbname] if p]
    return "/".join(parts)


@dataclass
class RetentionPolicy:
    keep_last: int = 0
    keep_daily: int = 0
    keep_weekly: int = 0
    keep_monthly: int = 0
    keep_yearly: int = 0


@dataclass
class Job:
    name: str
    datasource: Datasource
    store_config: dict
    prefix: str
    retention: RetentionPolicy = field(default_factory=RetentionPolicy)


def load(config_path: str | None = None) -> dict:
    """Load and parse the YAML config file."""
    path = config_path or os.environ.get("DBBACKUP_CONFIG", DEFAULT_CONFIG_PATH)

    if not Path(path).is_file():
        print(f"Error: config file not found: {path}", file=sys.stderr)
        sys.exit(1)

    # Warn if config file is readable by group or others (may contain credentials)
    try:
        mode = os.stat(path).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            log.warning(
                "Config file '%s' is readable by group/others (mode %o). "
                "This file may contain credentials — consider: chmod 600 %s",
                path, stat.S_IMODE(mode), path,
            )
    except OSError:
        pass  # skip check if stat fails (e.g. on some platforms)

    with open(path) as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        print(f"Error: config file must be a YAML mapping", file=sys.stderr)
        sys.exit(1)

    return raw


def resolve_env(config: dict) -> dict:
    """Recursively resolve *_env keys from environment variables.

    For any key ending in '_env', look up the env var named by its value
    and replace with a key without the '_env' suffix.
    E.g. {'password_env': 'MY_SECRET'} -> {'password': '<value of $MY_SECRET>'}
    """
    resolved = {}
    for key, value in config.items():
        if isinstance(value, dict):
            resolved[key] = resolve_env(value)
        elif isinstance(value, str) and key.endswith("_env"):
            real_key = key.removesuffix("_env")
            env_val = os.environ.get(value)
            if env_val is None:
                print(
                    f"Error: environment variable '{value}' "
                    f"(referenced by '{key}') is not set",
                    file=sys.stderr,
                )
                sys.exit(1)
            resolved[real_key] = env_val
        else:
            resolved[key] = value
    return resolved


def get_datasource(raw_config: dict, name: str) -> Datasource:
    """Get a Datasource by name from the config."""
    datasources = raw_config.get("datasources", {})
    if name not in datasources:
        print(
            f"Error: datasource '{name}' not found. "
            f"Available: {', '.join(datasources)}",
            file=sys.stderr,
        )
        sys.exit(1)

    ds = resolve_env(datasources[name])

    engine = ds.get("engine")
    if not engine:
        print(
            f"Error: datasource '{name}' is missing required 'engine' field "
            f"(e.g. engine: postgres)",
            file=sys.stderr,
        )
        sys.exit(1)

    # Collect engine-specific options (anything not in the standard keys)
    options = {k: v for k, v in ds.items() if k not in _DS_STANDARD_KEYS}

    for required in ("port", "database"):
        if required not in ds:
            print(
                f"Error: datasource '{name}' is missing required '{required}' field",
                file=sys.stderr,
            )
            sys.exit(1)

    return Datasource(
        name=name,
        engine=engine,
        host=ds.get("host", "localhost"),
        port=int(ds["port"]),
        user=ds.get("user", ""),
        password=ds.get("password", ""),
        database=ds["database"],
        options=options,
    )


def get_store_config(raw_config: dict, name: str) -> dict:
    """Get a resolved store config dict by name."""
    stores = raw_config.get("stores", {})
    if name not in stores:
        print(
            f"Error: store '{name}' not found. Available: {', '.join(stores)}",
            file=sys.stderr,
        )
        sys.exit(1)

    return resolve_env(stores[name])


def get_job(raw_config: dict, name: str) -> Job:
    """Get a fully resolved Job by name."""
    jobs = raw_config.get("jobs", {})
    if name not in jobs:
        print(
            f"Error: job '{name}' not found. Available: {', '.join(jobs)}",
            file=sys.stderr,
        )
        sys.exit(1)

    job_cfg = jobs[name]
    ds = get_datasource(raw_config, job_cfg["datasource"])
    store_cfg = get_store_config(raw_config, job_cfg["store"])

    ret_cfg = job_cfg.get("retention", {})
    retention = RetentionPolicy(
        keep_last=ret_cfg.get("keep_last", 0),
        keep_daily=ret_cfg.get("keep_daily", 0),
        keep_weekly=ret_cfg.get("keep_weekly", 0),
        keep_monthly=ret_cfg.get("keep_monthly", 0),
        keep_yearly=ret_cfg.get("keep_yearly", 0),
    )

    return Job(
        name=name,
        datasource=ds,
        store_config=store_cfg,
        prefix=job_cfg.get("prefix", ""),
        retention=retention,
    )


def get_all_job_names(raw_config: dict) -> list[str]:
    """Return all job names defined in the config."""
    return list(raw_config.get("jobs", {}).keys())
