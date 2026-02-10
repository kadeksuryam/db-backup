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
from utils import sha256_file

log = logging.getLogger(__name__)


def run_backup(ds: Datasource, store: Store, prefix: str, verify: bool = False) -> str:
    """Run a full backup cycle: dump -> upload -> optionally verify.

    Returns the remote key of the uploaded backup.
    """
    engine = create_engine(ds.engine)

    engine.check_connectivity(ds)
    engine.check_version_compat(ds)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    filename = f"{ds.database}-{timestamp}{engine.file_extension(ds)}"
    remote_key = f"{build_prefix(prefix, ds.database)}/{filename}"

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, filename)

        log.info("Starting backup for '%s' (engine: %s)...", ds.database, ds.engine)
        start = time.monotonic()

        engine.dump(ds, local_path)

        elapsed = time.monotonic() - start
        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        log.info("Dump completed in %.1fs (%.1f MB compressed)", elapsed, size_mb)

        if os.path.getsize(local_path) == 0:
            raise RuntimeError(
                f"Dump produced an empty (0-byte) file for '{ds.database}'. "
                f"This could indicate a problem with the database or engine."
            )

        store.upload(local_path, remote_key)

        checksum = sha256_file(local_path)
        checksum_path = local_path + ".sha256"
        with open(checksum_path, "w") as f:
            f.write(checksum)
        store.upload(checksum_path, remote_key + ".sha256")
        log.info("SHA256 sidecar uploaded: %s", checksum)

        if verify:
            verify_path = os.path.join(tmpdir, f"verify-{filename}")
            log.info("Verifying backup: %s", remote_key)
            store.download(remote_key, verify_path)
            engine.verify(ds, verify_path)
            log.info("Backup verification passed.")

    log.info("Backup complete: %s", remote_key)
    return remote_key
