"""SSH/scp storage backend."""

from __future__ import annotations

import logging
import os
import shlex
import shutil
import subprocess
import tempfile

from config import ConfigError

from . import BACKUP_EXTENSIONS, BackupInfo, Store, parse_timestamp

log = logging.getLogger(__name__)


class SSHStore(Store):
    def __init__(
        self,
        host: str,
        user: str,
        path: str,
        port: int = 22,
        key_file: str | None = None,
    ):
        self._host = host
        self._user = user
        self._base_path = path
        self._port = port
        self._key_file = key_file
        self._control_dir = tempfile.mkdtemp(prefix="dbbackup-ssh-")
        self._control_path = os.path.join(self._control_dir, "ctrl-%h-%p-%r")

    def _connect_opts(self, port_flag: str) -> list[str]:
        """Build common SSH/SCP options. port_flag is '-p' for ssh, '-P' for scp."""
        opts = [
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=10",
            "-o", f"ControlPath={self._control_path}",
            "-o", "ControlMaster=auto",
            "-o", "ControlPersist=60",
            port_flag, str(self._port),
        ]
        if self._key_file:
            opts.extend(["-i", self._key_file])
        return opts

    def _ssh_opts(self) -> list[str]:
        return self._connect_opts("-p")

    def _scp_opts(self) -> list[str]:
        return self._connect_opts("-P")

    def _ssh_dest(self) -> str:
        return f"{self._user}@{self._host}"

    def _run(self, cmd: list[str]) -> subprocess.CompletedProcess:
        log.debug("Running: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f"Command failed (exit {result.returncode}): {' '.join(cmd)}\n"
                f"stderr: {result.stderr.strip()}"
            )
        return result

    def close(self) -> None:
        """Tear down the SSH ControlMaster connection and clean up."""
        if self._control_dir and os.path.isdir(self._control_dir):
            try:
                subprocess.run(
                    ["ssh", "-o", f"ControlPath={self._control_path}",
                     "-O", "exit", self._ssh_dest()],
                    capture_output=True, check=False,
                )
            except OSError:
                pass
            shutil.rmtree(self._control_dir, ignore_errors=True)
            self._control_dir = ""

    def __del__(self) -> None:
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def upload(self, local_path: str, remote_key: str) -> None:
        remote_dir = os.path.dirname(f"{self._base_path}/{remote_key}")
        dest = f"{self._ssh_dest()}:{self._base_path}/{remote_key}"

        # Ensure remote directory exists
        self._run(["ssh", *self._ssh_opts(), self._ssh_dest(), f"mkdir -p {shlex.quote(remote_dir)}"])

        log.info("Uploading %s -> %s:%s", local_path, self._host, remote_key)
        self._run(["scp", *self._scp_opts(), local_path, dest])

    def download(self, remote_key: str, local_path: str) -> None:
        src = f"{self._ssh_dest()}:{self._base_path}/{remote_key}"

        log.info("Downloading %s:%s -> %s", self._host, remote_key, local_path)
        self._run(["scp", *self._scp_opts(), src, local_path])

    def list(self, prefix: str) -> list[BackupInfo]:
        remote_dir = f"{self._base_path}/{prefix}"

        # POSIX-portable: find files then get size with wc -c
        quoted_dir = shlex.quote(remote_dir)
        # Build find expression matching all recognized backup extensions
        name_clauses = " -o ".join(
            f"-name '*{ext}'" for ext in BACKUP_EXTENSIONS
        )
        cmd = [
            "ssh", *self._ssh_opts(), self._ssh_dest(),
            f"find {quoted_dir} \\( {name_clauses} \\) -type f 2>/dev/null "
            f"| while IFS= read -r f; do "
            f"size=$(wc -c < \"$f\"); "
            f"echo \"$f\\t$size\"; "
            f"done",
        ]
        result = self._run(cmd)

        backups: list[BackupInfo] = []
        for line in result.stdout.strip().splitlines():
            if not line.strip():
                continue
            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue
            full_path, size_str = parts
            # key is relative to base_path
            key = full_path.removeprefix(self._base_path).lstrip("/")
            filename = os.path.basename(full_path)
            ts = parse_timestamp(filename)
            if ts is None:
                continue
            backups.append(
                BackupInfo(
                    key=key,
                    filename=filename,
                    timestamp=ts,
                    size=int(size_str.strip()),
                )
            )

        backups.sort(key=lambda b: b.timestamp)
        return backups

    def delete(self, remote_key: str) -> None:
        remote_path = f"{self._base_path}/{remote_key}"
        log.info("Deleting %s:%s", self._host, remote_path)
        self._run(["ssh", *self._ssh_opts(), self._ssh_dest(), f"rm -f {shlex.quote(remote_path)}"])


def create(config: dict) -> SSHStore:
    for key in ("host", "user", "path"):
        if key not in config:
            raise ConfigError(f"Error: SSH store config is missing required '{key}' field")
    return SSHStore(
        host=config["host"],
        user=config["user"],
        path=config["path"],
        port=config.get("port", 22),
        key_file=config.get("key_file"),
    )
