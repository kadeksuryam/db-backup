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
import concurrent.futures
import logging
import sys
import time

import config
from config import ConfigError
from backup import run_backup
from notifiers import create_notifier
from restore import RestoreAborted, RestoreError, list_backups, run_restore
from retention import apply_retention
from stores import create_store

log = logging.getLogger("dbbackup")


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _dispatch_notifications(job, raw_config, status, message):
    """Send notifications for a completed job. Never raises — logs warnings."""
    notifiers = {}
    for rule in job.notifications:
        if rule.on != "always" and rule.on != status:
            continue
        try:
            if rule.notifier_name not in notifiers:
                notifier_cfg = config.get_notifier_config(raw_config, rule.notifier_name)
                notifiers[rule.notifier_name] = create_notifier(notifier_cfg)
            notifiers[rule.notifier_name].send(job.name, status, message)
        except Exception as exc:
            log.warning("Failed to send notification '%s' for job '%s': %s",
                        rule.notifier_name, job.name, exc)


def _run_single_job(name: str, raw_config: dict, prune: bool, dry_run: bool = False) -> None:
    """Run a single backup job. Self-contained — no shared mutable state."""
    job = config.get_job(raw_config, name)
    with create_store(job.store_config) as store:
        log.info("=== Job: %s ===", name)

        last_exc = None
        for attempt in range(1, job.retry.max_attempts + 1):
            try:
                if attempt > 1:
                    delay = job.retry.delay * (job.retry.backoff_multiplier ** (attempt - 2))
                    log.warning("Job '%s' attempt %d/%d failed. Retrying in %.0fs...",
                                name, attempt - 1, job.retry.max_attempts, delay)
                    time.sleep(delay)
                    log.info("=== Job: %s (attempt %d/%d) ===", name, attempt, job.retry.max_attempts)
                run_backup(job.datasource, store, job.prefix, verify=job.verify)
                last_exc = None
                break
            except ConfigError:
                raise  # never retry config errors
            except Exception as exc:
                last_exc = exc
                if attempt == job.retry.max_attempts:
                    break

        if last_exc is not None:
            _dispatch_notifications(job, raw_config, "failure", str(last_exc))
            raise last_exc

        if prune:
            apply_retention(store, job.prefix, job.datasource.database, job.retention, dry_run=dry_run)

        _dispatch_notifications(job, raw_config, "success", f"Backup completed for job '{name}'.")


def cmd_backup(args: argparse.Namespace, raw_config: dict) -> None:
    if args.all:
        job_names = config.get_all_job_names(raw_config)
        if not job_names:
            log.error("No jobs defined in config.")
            sys.exit(1)
        log.info("Running all jobs: %s", ", ".join(job_names))
    else:
        job_names = [args.job]

    parallel = getattr(args, "parallel", 1)
    dry_run = getattr(args, "dry_run", False)
    failed = []
    succeeded = []
    total_start = time.monotonic()

    if parallel <= 1:
        for name in job_names:
            job_start = time.monotonic()
            try:
                _run_single_job(name, raw_config, args.prune, dry_run=dry_run)
                succeeded.append((name, time.monotonic() - job_start))
            except Exception as e:
                log.error("Job '%s' failed: %s", name, e)
                failed.append(name)
    else:
        job_starts: dict[str, float] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as executor:
            future_to_name = {}
            for name in job_names:
                job_starts[name] = time.monotonic()
                future_to_name[
                    executor.submit(_run_single_job, name, raw_config, args.prune, dry_run=dry_run)
                ] = name
            for future in concurrent.futures.as_completed(future_to_name):
                name = future_to_name[future]
                elapsed = time.monotonic() - job_starts[name]
                try:
                    future.result()
                    succeeded.append((name, elapsed))
                except Exception as e:
                    log.error("Job '%s' failed: %s", name, e)
                    failed.append(name)

    # Summary (always log when running multiple jobs)
    total_elapsed = time.monotonic() - total_start
    if len(job_names) > 1:
        log.info(
            "=== Summary: %d succeeded, %d failed, total time %.1fs ===",
            len(succeeded), len(failed), total_elapsed,
        )
        for name, elapsed in succeeded:
            log.info("  OK   %s (%.1fs)", name, elapsed)
        for name in failed:
            log.info("  FAIL %s", name)

    if failed:
        log.error("Failed jobs: %s", ", ".join(failed))
        sys.exit(1)


def cmd_prune(args: argparse.Namespace, raw_config: dict) -> None:
    job = config.get_job(raw_config, args.job)
    dry_run = getattr(args, "dry_run", False)
    with create_store(job.store_config) as store:
        apply_retention(store, job.prefix, job.datasource.database, job.retention, dry_run=dry_run)


def cmd_list(args: argparse.Namespace, raw_config: dict) -> None:
    job = config.get_job(raw_config, args.job)
    with create_store(job.store_config) as store:
        list_backups(store, job.prefix, job.datasource.database)


def cmd_restore(args: argparse.Namespace, raw_config: dict) -> None:
    job = config.get_job(raw_config, args.job)
    with create_store(job.store_config) as store:
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
    p_backup.add_argument("--parallel", type=int, default=1, metavar="N",
        help="Run up to N backup jobs in parallel (default: 1, sequential)")
    p_backup.add_argument("--dry-run", action="store_true",
        help="Show what prune would delete without actually deleting")

    # prune
    p_prune = subparsers.add_parser("prune", help="Apply retention policy")
    p_prune.add_argument("job", help="Job name from config")
    p_prune.add_argument("--dry-run", action="store_true",
        help="Show what would be deleted without actually deleting")

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

    try:
        raw_config = config.load(args.config)

        commands = {
            "backup": cmd_backup,
            "prune": cmd_prune,
            "list": cmd_list,
            "restore": cmd_restore,
        }
        commands[args.command](args, raw_config)
    except RestoreAborted as e:
        print(str(e))
        sys.exit(0)
    except (ConfigError, RestoreError) as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
