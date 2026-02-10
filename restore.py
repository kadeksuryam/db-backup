"""Download a backup from a store and restore it into a database."""

from __future__ import annotations

import logging
import os
import sys
import tempfile

from config import Datasource, build_prefix
from engines import create_engine
from stores import Store
from utils import sha256_file

log = logging.getLogger(__name__)


def list_backups(store: Store, prefix: str, dbname: str) -> None:
    """Print available backups for a job."""
    full_prefix = build_prefix(prefix, dbname)
    backups = store.list(full_prefix)

    if not backups:
        print(f"No backups found under '{full_prefix}'")
        return

    print(f"{'Timestamp':<22} {'Size':>10}  {'Key'}")
    print("-" * 70)
    for b in backups:
        ts_str = b.timestamp.strftime("%Y-%m-%d %H:%M:%S")
        if b.size >= 1024 * 1024 * 1024:
            size_str = f"{b.size / (1024**3):.1f} GB"
        elif b.size >= 1024 * 1024:
            size_str = f"{b.size / (1024**2):.1f} MB"
        elif b.size >= 1024:
            size_str = f"{b.size / 1024:.1f} KB"
        else:
            size_str = f"{b.size} B"
        print(f"{ts_str:<22} {size_str:>10}  {b.key}")

    print(f"\nTotal: {len(backups)} backup(s)")


def run_restore(
    ds: Datasource,
    store: Store,
    prefix: str,
    filename: str | None = None,
    auto_confirm: bool = False,
) -> None:
    """Download and restore a backup.

    If filename is None, restores the latest backup.
    """
    engine = create_engine(ds.engine)
    full_prefix = build_prefix(prefix, ds.database)
    backups = store.list(full_prefix)

    if not backups:
        print(f"No backups found under '{full_prefix}'", file=sys.stderr)
        sys.exit(1)

    if filename:
        # Find the matching backup
        match = [b for b in backups if b.filename == filename]
        if not match:
            print(f"Backup '{filename}' not found. Use 'list' to see available backups.", file=sys.stderr)
            sys.exit(1)
        target = match[0]
    else:
        target = backups[-1]  # latest (list is sorted oldest-first)

    log.info("Selected backup: %s (%s)", target.filename, target.timestamp.strftime("%Y-%m-%d %H:%M:%S"))

    # Download and verify first (before touching the database)
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, target.filename)
        store.download(target.key, local_path)

        log.info("Verifying backup integrity: %s", target.filename)
        engine.verify(ds, local_path)

        try:
            checksum_path = os.path.join(tmpdir, target.filename + ".sha256")
            store.download(target.key + ".sha256", checksum_path)
            with open(checksum_path) as f:
                expected = f.read().strip()
            if len(expected) != 64:
                raise ValueError("invalid sidecar")
            actual = sha256_file(local_path)
            if actual != expected:
                raise RuntimeError(
                    f"Checksum mismatch: expected {expected}, got {actual}")
            log.info("SHA256 checksum verified.")
        except RuntimeError as e:
            if "Checksum mismatch" in str(e):
                raise
            log.info("No valid SHA256 sidecar — skipping checksum verification.")
        except Exception:
            log.info("No SHA256 sidecar found — skipping checksum verification.")

        # Check existing data (only after download+verify succeed)
        table_count = engine.count_tables(ds)
        if table_count > 0:
            if auto_confirm:
                log.info("Auto-confirming database drop (--auto-confirm).")
            else:
                print(f"Database '{ds.database}' already contains {table_count} table(s).")
                print(f"Connection: {ds.engine}://{ds.user}@{ds.host}:{ds.port}/{ds.database}")
                answer = input("Drop and recreate the database? [y/N]: ").strip()
                if answer.lower() not in ("y", "yes"):
                    print("Restore aborted.")
                    sys.exit(0)

            log.info("Dropping and recreating database '%s'...", ds.database)
            engine.drop_and_recreate(ds)

        log.info("Restoring '%s' into '%s' (engine: %s)...", target.filename, ds.database, ds.engine)
        engine.restore(ds, local_path)

    log.info("Restore complete.")
