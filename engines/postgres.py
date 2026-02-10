"""PostgreSQL engine — pg_dump/psql based backup and restore."""

from __future__ import annotations

import logging
import os
import re
import subprocess

from config import Datasource
from . import Engine

log = logging.getLogger(__name__)


_SAFE_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_]+$")


def _validate_identifier(name: str) -> None:
    """Reject identifiers that are not safe for SQL interpolation."""
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Unsafe database identifier: {name!r}. "
            f"Only alphanumeric characters and underscores are allowed."
        )


# Mapping: compression name → (compress_cmd_template, decompress_cmd_template, extension, default_level)
# Templates use {level} as a placeholder for the level flag.
_COMPRESSION_TOOLS: dict[str, tuple[list[str], list[str], str, int]] = {
    "gzip":  (["gzip", "-{level}"],        ["gunzip", "-c"],    ".gz",  6),
    "zstd":  (["zstd", "-{level}", "-c"],   ["zstd", "-d", "-c"], ".zst", 3),
    "lz4":   (["lz4", "-{level}", "-c"],    ["lz4", "-d", "-c"],  ".lz4", 1),
}

_VALID_FORMATS = {"plain", "custom"}

# Extension → (format, compression) mapping for restore detection.
_EXTENSION_MAP: dict[str, tuple[str, str]] = {
    ".sql.gz":   ("plain",  "gzip"),
    ".sql.zst":  ("plain",  "zstd"),
    ".sql.lz4":  ("plain",  "lz4"),
    ".sql":      ("plain",  "none"),
    ".dump.gz":  ("custom", "gzip"),
    ".dump.zst": ("custom", "zstd"),
    ".dump.lz4": ("custom", "lz4"),
    ".dump":     ("custom", "none"),
}


def _resolve_format(ds: Datasource) -> str:
    """Return the dump format from datasource options, defaulting to 'plain'."""
    fmt = ds.options.get("format", "plain")
    if fmt not in _VALID_FORMATS:
        raise ValueError(
            f"Invalid format '{fmt}'. Supported: {', '.join(sorted(_VALID_FORMATS))}"
        )
    return fmt


def _resolve_compression(ds: Datasource) -> tuple[list[str] | None, list[str] | None, str]:
    """Return (compress_cmd, decompress_cmd, extension) from datasource options.

    Returns (None, None, "") when compression is "none".
    """
    compression = ds.options.get("compression", "gzip")
    if compression == "none":
        return None, None, ""
    if compression not in _COMPRESSION_TOOLS:
        raise ValueError(
            f"Invalid compression '{compression}'. "
            f"Supported: {', '.join(sorted(_COMPRESSION_TOOLS))}, none"
        )
    compress_tpl, decompress_cmd, ext, default_level = _COMPRESSION_TOOLS[compression]
    level = ds.options.get("compression_level", default_level)
    compress_cmd = [part.replace("{level}", str(level)) for part in compress_tpl]
    return compress_cmd, list(decompress_cmd), ext


def _detect_from_extension(filename: str) -> tuple[str, str]:
    """Detect (format, compression) from a backup filename extension.

    Raises ValueError for unrecognized extensions.
    """
    # Check compound extensions first (they are listed first in the map)
    for ext, (fmt, comp) in _EXTENSION_MAP.items():
        if filename.endswith(ext):
            return fmt, comp
    raise ValueError(f"Unrecognized backup file extension: {filename}")


class PostgresEngine(Engine):

    # -- private helpers --------------------------------------------------

    @staticmethod
    def _pg_bin(ds: Datasource, name: str) -> str:
        """Return path to a PG binary, respecting the pg_version option."""
        pg_ver = ds.options.get("pg_version")
        if pg_ver is not None:
            return f"/usr/lib/postgresql/{int(pg_ver)}/bin/{name}"
        return name

    @staticmethod
    def _pg_env(ds: Datasource) -> dict[str, str]:
        """Build environment dict for PostgreSQL CLI tools."""
        env = os.environ.copy()
        env["PGHOST"] = ds.host
        env["PGPORT"] = str(ds.port)
        env["PGUSER"] = ds.user
        env["PGPASSWORD"] = ds.password
        env["PGDATABASE"] = ds.database
        return env

    # -- Engine interface -------------------------------------------------

    def check_connectivity(self, ds: Datasource) -> None:
        log.info("Checking database connectivity: %s@%s:%d/%s", ds.user, ds.host, ds.port, ds.database)
        result = subprocess.run(
            [self._pg_bin(ds, "pg_isready"), "-h", ds.host, "-p", str(ds.port), "-U", ds.user, "-d", ds.database],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Database is not reachable: {result.stdout.strip()} {result.stderr.strip()}"
            )
        log.info("Database is ready.")

    def check_version_compat(self, ds: Datasource) -> None:
        # Client major version
        result = subprocess.run(
            [self._pg_bin(ds, "pg_dump"), "--version"], capture_output=True, text=True
        )
        client_match = re.search(r"(\d+)", result.stdout)
        if not client_match:
            return
        client_major = int(client_match.group(1))

        # Server major version
        result = subprocess.run(
            [self._pg_bin(ds, "psql"), "-tAc", "SHOW server_version_num;"],
            env=self._pg_env(ds),
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return
        try:
            server_ver_num = int(result.stdout.strip())
            server_major = server_ver_num // 10000
        except ValueError:
            return

        log.info("PostgreSQL client: %d, server: %d", client_major, server_major)
        if client_major < server_major:
            log.warning(
                "pg_dump client version (%d) is older than server (%d). "
                "This may cause errors or missing features. "
                "Set pg_version: %d on this datasource and rebuild with: "
                "--build-arg PG_VERSIONS=\"%d\"",
                client_major,
                server_major,
                server_major,
                server_major,
            )

    def dump(self, ds: Datasource, output_path: str) -> None:
        pg_env = self._pg_env(ds)
        fmt = _resolve_format(ds)
        compress_cmd, _, _ = _resolve_compression(ds)

        pg_dump_cmd = [self._pg_bin(ds, "pg_dump"), "--no-owner", "--no-privileges"]
        if fmt == "custom":
            pg_dump_cmd.extend(["-Fc", "-Z0"])

        # Open with 0o600 to prevent other users from reading database dumps
        fd = os.open(output_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as outfile:
            if compress_cmd is not None:
                dump_proc = subprocess.Popen(
                    pg_dump_cmd,
                    env=pg_env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                compress_proc = subprocess.Popen(
                    compress_cmd,
                    stdin=dump_proc.stdout,
                    stdout=outfile,
                    stderr=subprocess.PIPE,
                )
                # Allow dump to receive SIGPIPE if compressor exits early
                dump_proc.stdout.close()
                compress_proc.wait()
                dump_proc.wait()
            else:
                # No compression — write pg_dump output directly
                dump_proc = subprocess.Popen(
                    pg_dump_cmd,
                    env=pg_env,
                    stdout=outfile,
                    stderr=subprocess.PIPE,
                )
                dump_proc.wait()

        if dump_proc.returncode != 0:
            stderr = dump_proc.stderr.read().decode().strip() if dump_proc.stderr else ""
            raise RuntimeError(f"pg_dump failed (exit {dump_proc.returncode}): {stderr}")

    def restore(self, ds: Datasource, input_path: str) -> None:
        env = self._pg_env(ds)

        # Detect format and compression from the file extension, not from
        # ds.options — this allows restoring old backups with different settings.
        fmt, compression = _detect_from_extension(input_path)

        if fmt == "plain":
            restore_cmd = [
                self._pg_bin(ds, "psql"),
                "--single-transaction", "--set", "ON_ERROR_STOP=1",
            ]
        else:
            restore_cmd = [
                self._pg_bin(ds, "pg_restore"),
                "--no-owner", "--no-privileges",
            ]

        if compression != "none":
            _, decompress_cmd, _, _ = _COMPRESSION_TOOLS[compression]
            decompress_cmd = list(decompress_cmd)

            with open(input_path, "rb") as infile:
                decompress_proc = subprocess.Popen(
                    decompress_cmd,
                    stdin=infile,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                restore_proc = subprocess.Popen(
                    restore_cmd,
                    env=env,
                    stdin=decompress_proc.stdout,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                decompress_proc.stdout.close()
                restore_out, restore_err = restore_proc.communicate()
                decompress_proc.wait()
        else:
            with open(input_path, "rb") as infile:
                restore_proc = subprocess.Popen(
                    restore_cmd,
                    env=env,
                    stdin=infile,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                restore_out, restore_err = restore_proc.communicate()

        if restore_proc.returncode != 0:
            tool = "psql" if fmt == "plain" else "pg_restore"
            raise RuntimeError(
                f"{tool} restore failed (exit {restore_proc.returncode}): {restore_err.decode().strip()}"
            )

    def count_tables(self, ds: Datasource) -> int:
        result = subprocess.run(
            [
                self._pg_bin(ds, "psql"), "-tAc",
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public';",
            ],
            env=self._pg_env(ds),
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return 0
        return int(result.stdout.strip() or "0")

    def drop_and_recreate(self, ds: Datasource) -> None:
        _validate_identifier(ds.database)

        env = self._pg_env(ds)
        # Connect to 'postgres' db to drop the target
        env["PGDATABASE"] = "postgres"

        # Safe: database name is validated to contain only [a-zA-Z0-9_]
        db = ds.database
        sql = (
            f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            f"WHERE datname = '{db}';\n"
            f"DROP DATABASE IF EXISTS \"{db}\";\n"
            f"CREATE DATABASE \"{db}\";\n"
        )
        result = subprocess.run(
            [self._pg_bin(ds, "psql"), "-c", sql],
            env=env,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Failed to recreate database: {result.stderr.strip()}")

    def file_extension(self, ds: Datasource) -> str:
        fmt = _resolve_format(ds)
        _, _, comp_ext = _resolve_compression(ds)
        base = ".sql" if fmt == "plain" else ".dump"
        return f"{base}{comp_ext}"


def create() -> PostgresEngine:
    return PostgresEngine()
