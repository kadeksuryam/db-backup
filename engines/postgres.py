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

        # Open with 0o600 to prevent other users from reading database dumps
        fd = os.open(output_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as outfile:
            dump_proc = subprocess.Popen(
                [self._pg_bin(ds, "pg_dump"), "--no-owner", "--no-privileges"],
                env=pg_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            gzip_proc = subprocess.Popen(
                ["gzip", "-6"],
                stdin=dump_proc.stdout,
                stdout=outfile,
                stderr=subprocess.PIPE,
            )
            # Allow dump to receive SIGPIPE if gzip exits early
            dump_proc.stdout.close()
            gzip_proc.wait()
            dump_proc.wait()

        if dump_proc.returncode != 0:
            stderr = dump_proc.stderr.read().decode().strip() if dump_proc.stderr else ""
            raise RuntimeError(f"pg_dump failed (exit {dump_proc.returncode}): {stderr}")

    def restore(self, ds: Datasource, input_path: str) -> None:
        env = self._pg_env(ds)

        with open(input_path, "rb") as infile:
            gunzip = subprocess.Popen(
                ["gunzip", "-c"],
                stdin=infile,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            psql = subprocess.Popen(
                [self._pg_bin(ds, "psql"), "--single-transaction", "--set", "ON_ERROR_STOP=1"],
                env=env,
                stdin=gunzip.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            gunzip.stdout.close()
            psql_out, psql_err = psql.communicate()
            gunzip.wait()

        if psql.returncode != 0:
            raise RuntimeError(
                f"psql restore failed (exit {psql.returncode}): {psql_err.decode().strip()}"
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

    def file_extension(self) -> str:
        return ".sql.gz"


def create() -> PostgresEngine:
    return PostgresEngine()
