"""GFS (Grandfather-Father-Son) retention policy for backup pruning.

Mirrors the retention model used by Proxmox Backup Server and sanoid:
- keep_last:    always keep the N most recent backups
- keep_daily:   keep the newest backup per day, for the last N days
- keep_weekly:  keep the newest backup per ISO week, for the last N weeks
- keep_monthly: keep the newest backup per month, for the last N months
- keep_yearly:  keep the newest backup per year, for the last N years

All fields are optional (default 0 = disabled).
A backup kept by any rule is protected from deletion.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from config import RetentionPolicy, build_prefix
from stores import BackupInfo, Store

log = logging.getLogger(__name__)


def _bucket_key_daily(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _bucket_key_weekly(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _bucket_key_monthly(dt: datetime) -> str:
    return dt.strftime("%Y-%m")


def _bucket_key_yearly(dt: datetime) -> str:
    return dt.strftime("%Y")


def compute_keep_set(
    backups: list[BackupInfo], policy: RetentionPolicy, now: datetime | None = None
) -> set[str]:
    """Determine which backup keys to keep based on the retention policy.

    Args:
        backups: list of BackupInfo, sorted oldest-first.
        policy: retention rules.
        now: reference time (defaults to utcnow).

    Returns:
        Set of backup keys that should be kept.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    keep: set[str] = set()

    if not backups:
        return keep

    # Newest-first for keep_last
    sorted_newest = sorted(backups, key=lambda b: b.timestamp, reverse=True)

    # keep_last: always keep the N most recent
    if policy.keep_last > 0:
        for b in sorted_newest[: policy.keep_last]:
            keep.add(b.key)

    # For daily/weekly/monthly/yearly: bucket backups by time period,
    # then keep the newest backup in each of the most recent N buckets.
    def _apply_bucket_rule(bucket_fn, count: int) -> None:
        if count <= 0:
            return

        buckets: dict[str, BackupInfo] = {}
        for b in backups:
            bkey = bucket_fn(b.timestamp)
            # Keep the newest backup per bucket
            if bkey not in buckets or b.timestamp > buckets[bkey].timestamp:
                buckets[bkey] = b

        # Sort bucket keys descending (most recent first) and take N
        sorted_keys = sorted(buckets.keys(), reverse=True)[:count]
        for bkey in sorted_keys:
            keep.add(buckets[bkey].key)

    _apply_bucket_rule(_bucket_key_daily, policy.keep_daily)
    _apply_bucket_rule(_bucket_key_weekly, policy.keep_weekly)
    _apply_bucket_rule(_bucket_key_monthly, policy.keep_monthly)
    _apply_bucket_rule(_bucket_key_yearly, policy.keep_yearly)

    return keep


def apply_retention(
    store: Store, prefix: str, dbname: str, policy: RetentionPolicy
) -> None:
    """List backups, compute retention, and delete expired ones."""
    full_prefix = build_prefix(prefix, dbname)

    backups = store.list(full_prefix)

    if not backups:
        log.info("No backups found under '%s', nothing to prune.", full_prefix)
        return

    # If no retention rules are configured, keep everything
    has_rules = any([
        policy.keep_last,
        policy.keep_daily,
        policy.keep_weekly,
        policy.keep_monthly,
        policy.keep_yearly,
    ])
    if not has_rules:
        log.info("No retention policy configured, keeping all %d backup(s).", len(backups))
        return

    keep = compute_keep_set(backups, policy)
    to_delete = [b for b in backups if b.key not in keep]

    log.info(
        "Retention: %d total, %d to keep, %d to delete",
        len(backups),
        len(keep),
        len(to_delete),
    )

    for b in to_delete:
        log.info("Deleting expired backup: %s (%s)", b.filename, b.timestamp.strftime("%Y-%m-%d %H:%M:%S"))
        store.delete(b.key)

    if to_delete:
        log.info("Pruned %d expired backup(s).", len(to_delete))
    else:
        log.info("No expired backups to prune.")
