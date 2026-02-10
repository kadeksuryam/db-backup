#!/usr/bin/env python3
"""dbbackup — database backup tool with pluggable engines, storage, and GFS retention.

Usage:
    dbbackup backup <job> [--prune]
    dbbackup backup --all [--prune]
    dbbackup prune <job>
    dbbackup list <job>
    dbbackup restore <job> [<filename>] [--auto-confirm]
"""

from __future__ import annotations

import argparse
import logging
import sys

import config
from backup import run_backup
from restore import list_backups, run_restore
from retention import apply_retention
from stores import create_store

log = logging.getLogger("dbbackup")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def cmd_backup(args: argparse.Namespace, raw_config: dict) -> None:
    if args.all:
        job_names = config.get_all_job_names(raw_config)
        if not job_names:
            log.error("No jobs defined in config.")
            sys.exit(1)
        log.info("Running all jobs: %s", ", ".join(job_names))
    else:
        job_names = [args.job]

    failed = []
    for name in job_names:
        try:
            job = config.get_job(raw_config, name)
            store = create_store(job.store_config)
            log.info("=== Job: %s ===", name)
            run_backup(job.datasource, store, job.prefix)
            if args.prune:
                apply_retention(store, job.prefix, job.datasource.database, job.retention)
        except Exception as e:
            log.error("Job '%s' failed: %s", name, e)
            failed.append(name)

    if failed:
        log.error("Failed jobs: %s", ", ".join(failed))
        sys.exit(1)


def cmd_prune(args: argparse.Namespace, raw_config: dict) -> None:
    job = config.get_job(raw_config, args.job)
    store = create_store(job.store_config)
    apply_retention(store, job.prefix, job.datasource.database, job.retention)


def cmd_list(args: argparse.Namespace, raw_config: dict) -> None:
    job = config.get_job(raw_config, args.job)
    store = create_store(job.store_config)
    list_backups(store, job.prefix, job.datasource.database)


def cmd_restore(args: argparse.Namespace, raw_config: dict) -> None:
    job = config.get_job(raw_config, args.job)
    store = create_store(job.store_config)
    run_restore(
        job.datasource,
        store,
        job.prefix,
        filename=args.filename,
        auto_confirm=args.auto_confirm,
    )


def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(
        prog="dbbackup",
        description="Database backup tool with pluggable engines, storage, and GFS retention.",
    )
    parser.add_argument(
        "-c", "--config",
        default=None,
        help=f"Config file path (default: $DBBACKUP_CONFIG or {config.DEFAULT_CONFIG_PATH})",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # backup
    p_backup = subparsers.add_parser("backup", help="Run a backup job")
    p_backup.add_argument("job", nargs="?", help="Job name from config")
    p_backup.add_argument("--all", action="store_true", help="Run all jobs")
    p_backup.add_argument("--prune", action="store_true", help="Prune after backup")

    # prune
    p_prune = subparsers.add_parser("prune", help="Apply retention policy")
    p_prune.add_argument("job", help="Job name from config")

    # list
    p_list = subparsers.add_parser("list", help="List available backups")
    p_list.add_argument("job", help="Job name from config")

    # restore
    p_restore = subparsers.add_parser("restore", help="Restore a backup")
    p_restore.add_argument("job", help="Job name from config")
    p_restore.add_argument("filename", nargs="?", default=None, help="Specific backup filename (default: latest)")
    p_restore.add_argument("--auto-confirm", action="store_true", help="Skip confirmation prompt for drop/recreate")

    args = parser.parse_args()

    # Validate backup command args
    if args.command == "backup" and not args.all and not args.job:
        parser.error("backup requires a job name or --all")

    raw_config = config.load(args.config)

    commands = {
        "backup": cmd_backup,
        "prune": cmd_prune,
        "list": cmd_list,
        "restore": cmd_restore,
    }
    commands[args.command](args, raw_config)


if __name__ == "__main__":
    main()
