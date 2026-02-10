"""Database backup: dump -> upload to store."""

from __future__ import annotations

import logging
import os
import tempfile
import time
from datetime import datetime, timezone

from config import Datasource, build_prefix
from engines import create_engine
from stores import Store

log = logging.getLogger(__name__)


def run_backup(ds: Datasource, store: Store, prefix: str) -> str:
    """Run a full backup cycle: dump -> upload.

    Returns the remote key of the uploaded backup.
    """
    engine = create_engine(ds.engine)

    engine.check_connectivity(ds)
    engine.check_version_compat(ds)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    filename = f"{ds.database}-{timestamp}{engine.file_extension()}"
    remote_key = f"{build_prefix(prefix, ds.database)}/{filename}"

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, filename)

        log.info("Starting backup for '%s' (engine: %s)...", ds.database, ds.engine)
        start = time.monotonic()

        engine.dump(ds, local_path)

        elapsed = time.monotonic() - start
        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        log.info("Dump completed in %.1fs (%.1f MB compressed)", elapsed, size_mb)

        store.upload(local_path, remote_key)

    log.info("Backup complete: %s", remote_key)
    return remote_key
