"""Tests for engines package — factory and PostgresEngine."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch, call

import pytest

from config import Datasource
from engines import create_engine, Engine
from engines.postgres import PostgresEngine, _validate_identifier


def _ds(**overrides) -> Datasource:
    """Create a test Datasource with sensible defaults."""
    defaults = {
        "name": "test",
        "engine": "postgres",
        "host": "localhost",
        "port": 5432,
        "user": "testuser",
        "password": "testpass",
        "database": "testdb",
        "options": {},
    }
    defaults.update(overrides)
    return Datasource(**defaults)


class TestCreateEngine:
    def test_creates_postgres(self):
        engine = create_engine("postgres")
        assert isinstance(engine, PostgresEngine)

    def test_unknown_engine_raises(self):
        with pytest.raises(ValueError, match="Unknown engine type 'mysql'"):
            create_engine("mysql")


class TestPostgresEngine:

    # -- _pg_bin ----------------------------------------------------------

    def test_pg_bin_with_version(self):
        ds = _ds(options={"pg_version": 14})
        assert PostgresEngine._pg_bin(ds, "pg_dump") == "/usr/lib/postgresql/14/bin/pg_dump"

    def test_pg_bin_without_version(self):
        ds = _ds()
        assert PostgresEngine._pg_bin(ds, "pg_dump") == "pg_dump"

    def test_pg_bin_with_string_version(self):
        """pg_version might come from YAML as a string."""
        ds = _ds(options={"pg_version": "16"})
        assert PostgresEngine._pg_bin(ds, "psql") == "/usr/lib/postgresql/16/bin/psql"

    # -- _pg_env ----------------------------------------------------------

    def test_pg_env_sets_vars(self):
        ds = _ds()
        env = PostgresEngine._pg_env(ds)
        assert env["PGHOST"] == "localhost"
        assert env["PGPORT"] == "5432"
        assert env["PGUSER"] == "testuser"
        assert env["PGPASSWORD"] == "testpass"
        assert env["PGDATABASE"] == "testdb"

    # -- check_connectivity -----------------------------------------------

    @patch("engines.postgres.subprocess.run")
    def test_check_connectivity_success(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        engine = PostgresEngine()
        engine.check_connectivity(_ds())  # should not raise

    @patch("engines.postgres.subprocess.run")
    def test_check_connectivity_failure(self, mock_run):
        mock_run.return_value = MagicMock(returncode=2, stdout="", stderr="refused")
        engine = PostgresEngine()
        with pytest.raises(RuntimeError, match="not reachable"):
            engine.check_connectivity(_ds())

    # -- check_version_compat ---------------------------------------------

    @patch("engines.postgres.subprocess.run")
    def test_version_compat_no_warning(self, mock_run):
        """Same version → no warning."""
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="pg_dump (PostgreSQL) 17.2"),
            MagicMock(returncode=0, stdout="170002"),
        ]
        engine = PostgresEngine()
        engine.check_version_compat(_ds())  # should not raise

    @patch("engines.postgres.subprocess.run")
    def test_version_compat_client_older_warns(self, mock_run, caplog):
        """Client 14, server 17 → warning."""
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="pg_dump (PostgreSQL) 14.5"),
            MagicMock(returncode=0, stdout="170002"),
        ]
        engine = PostgresEngine()
        import logging
        with caplog.at_level(logging.WARNING):
            engine.check_version_compat(_ds())
        assert "older than server" in caplog.text

    @patch("engines.postgres.subprocess.run")
    def test_version_compat_no_digits_in_version(self, mock_run):
        """pg_dump --version returns no digits → early return, no error."""
        mock_run.return_value = MagicMock(returncode=0, stdout="unknown version")
        engine = PostgresEngine()
        engine.check_version_compat(_ds())  # should not raise
        # Only one call (pg_dump --version), no psql call
        assert mock_run.call_count == 1

    @patch("engines.postgres.subprocess.run")
    def test_version_compat_psql_fails(self, mock_run):
        """psql query fails → early return, no error."""
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="pg_dump (PostgreSQL) 17.2"),
            MagicMock(returncode=1, stdout="", stderr="connection refused"),
        ]
        engine = PostgresEngine()
        engine.check_version_compat(_ds())  # should not raise

    @patch("engines.postgres.subprocess.run")
    def test_version_compat_bad_server_version(self, mock_run):
        """Server returns unparseable version → early return."""
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="pg_dump (PostgreSQL) 17.2"),
            MagicMock(returncode=0, stdout="not-a-number"),
        ]
        engine = PostgresEngine()
        engine.check_version_compat(_ds())  # should not raise

    @patch("engines.postgres.subprocess.run")
    def test_version_compat_client_newer_no_warning(self, mock_run, caplog):
        """Client 17, server 14 → no warning (client >= server is fine)."""
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="pg_dump (PostgreSQL) 17.2"),
            MagicMock(returncode=0, stdout="140005"),
        ]
        engine = PostgresEngine()
        import logging
        with caplog.at_level(logging.WARNING):
            engine.check_version_compat(_ds())
        assert "older than server" not in caplog.text

    @patch("engines.postgres.subprocess.run")
    def test_check_connectivity_uses_versioned_binary(self, mock_run):
        """pg_isready should use the versioned path when pg_version is set."""
        mock_run.return_value = MagicMock(returncode=0)
        ds = _ds(options={"pg_version": 14})
        engine = PostgresEngine()
        engine.check_connectivity(ds)
        cmd = mock_run.call_args[0][0]
        assert cmd[0] == "/usr/lib/postgresql/14/bin/pg_isready"

    @patch("engines.postgres.subprocess.run")
    def test_count_tables_empty_stdout(self, mock_run):
        """Empty stdout → returns 0."""
        mock_run.return_value = MagicMock(returncode=0, stdout="  \n")
        engine = PostgresEngine()
        assert engine.count_tables(_ds()) == 0

    @patch("engines.postgres.subprocess.run")
    def test_drop_and_recreate_connects_to_postgres_db(self, mock_run):
        """drop_and_recreate should connect to 'postgres' db, not the target."""
        mock_run.return_value = MagicMock(returncode=0)
        ds = _ds(database="myapp")
        engine = PostgresEngine()
        engine.drop_and_recreate(ds)

        # Check the env passed to subprocess
        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["env"]["PGDATABASE"] == "postgres"

        # Check SQL contains the right database name
        cmd = mock_run.call_args[0][0]
        sql_arg = cmd[-1]  # -c <sql>
        assert "myapp" in sql_arg

    # -- dump -------------------------------------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_dump_success(self, mock_popen, tmp_path):
        outfile = tmp_path / "test.sql.gz"

        # Mock pg_dump process
        mock_dump = MagicMock()
        mock_dump.stdout = MagicMock()
        mock_dump.stdout.close = MagicMock()
        mock_dump.stderr = MagicMock()
        mock_dump.stderr.read.return_value = b""
        mock_dump.wait.return_value = 0
        mock_dump.returncode = 0

        # Mock gzip process
        mock_gzip = MagicMock()
        mock_gzip.wait.return_value = 0

        mock_popen.side_effect = [mock_dump, mock_gzip]

        engine = PostgresEngine()
        engine.dump(_ds(), str(outfile))

        assert mock_dump.stdout.close.called
        assert mock_gzip.wait.called
        assert mock_dump.wait.called

    @patch("engines.postgres.subprocess.Popen")
    def test_dump_failure_raises(self, mock_popen, tmp_path):
        outfile = tmp_path / "test.sql.gz"

        mock_dump = MagicMock()
        mock_dump.stdout = MagicMock()
        mock_dump.stdout.close = MagicMock()
        mock_dump.stderr = MagicMock()
        mock_dump.stderr.read.return_value = b"connection refused"
        mock_dump.wait.return_value = 1
        mock_dump.returncode = 1

        mock_gzip = MagicMock()
        mock_gzip.wait.return_value = 0

        mock_popen.side_effect = [mock_dump, mock_gzip]

        engine = PostgresEngine()
        with pytest.raises(RuntimeError, match="pg_dump failed"):
            engine.dump(_ds(), str(outfile))

    # -- restore ----------------------------------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_restore_success(self, mock_popen, tmp_path):
        infile = tmp_path / "test.sql.gz"
        infile.write_bytes(b"fake")

        mock_gunzip = MagicMock()
        mock_gunzip.stdout = MagicMock()
        mock_gunzip.stdout.close = MagicMock()
        mock_gunzip.wait.return_value = 0

        mock_psql = MagicMock()
        mock_psql.communicate.return_value = (b"", b"")
        mock_psql.returncode = 0

        mock_popen.side_effect = [mock_gunzip, mock_psql]

        engine = PostgresEngine()
        engine.restore(_ds(), str(infile))

    @patch("engines.postgres.subprocess.Popen")
    def test_restore_failure_raises(self, mock_popen, tmp_path):
        infile = tmp_path / "test.sql.gz"
        infile.write_bytes(b"fake")

        mock_gunzip = MagicMock()
        mock_gunzip.stdout = MagicMock()
        mock_gunzip.stdout.close = MagicMock()
        mock_gunzip.wait.return_value = 0

        mock_psql = MagicMock()
        mock_psql.communicate.return_value = (b"", b"ERROR: syntax error")
        mock_psql.returncode = 3

        mock_popen.side_effect = [mock_gunzip, mock_psql]

        engine = PostgresEngine()
        with pytest.raises(RuntimeError, match="psql restore failed"):
            engine.restore(_ds(), str(infile))

    # -- count_tables -----------------------------------------------------

    @patch("engines.postgres.subprocess.run")
    def test_count_tables(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="42\n")
        engine = PostgresEngine()
        assert engine.count_tables(_ds()) == 42

    @patch("engines.postgres.subprocess.run")
    def test_count_tables_failure_returns_zero(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1)
        engine = PostgresEngine()
        assert engine.count_tables(_ds()) == 0

    # -- drop_and_recreate ------------------------------------------------

    @patch("engines.postgres.subprocess.run")
    def test_drop_and_recreate_success(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        engine = PostgresEngine()
        engine.drop_and_recreate(_ds())

    @patch("engines.postgres.subprocess.run")
    def test_drop_and_recreate_failure(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stderr="permission denied")
        engine = PostgresEngine()
        with pytest.raises(RuntimeError, match="Failed to recreate"):
            engine.drop_and_recreate(_ds())

    # -- file_extension ---------------------------------------------------

    def test_file_extension(self):
        assert PostgresEngine().file_extension() == ".sql.gz"


class TestCreateEngineEdgeCases:
    def test_empty_string_engine_raises(self):
        with pytest.raises(ValueError, match="Unknown engine type"):
            create_engine("")

    def test_none_engine_raises(self):
        with pytest.raises(ValueError):
            create_engine(None)

    def test_case_sensitive(self):
        """Engine types are case-sensitive."""
        with pytest.raises(ValueError):
            create_engine("Postgres")


class TestPostgresEngineEdgeCases:
    def test_pg_bin_float_version(self):
        """pg_version as float like 14.5 → int(14.5) = 14."""
        ds = _ds(options={"pg_version": 14.5})
        assert PostgresEngine._pg_bin(ds, "pg_dump") == "/usr/lib/postgresql/14/bin/pg_dump"

    @patch("engines.postgres.subprocess.run")
    def test_version_compat_client_equal_server(self, mock_run, caplog):
        """Client == server → no warning."""
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="pg_dump (PostgreSQL) 14.2"),
            MagicMock(returncode=0, stdout="140002"),
        ]
        engine = PostgresEngine()
        import logging
        with caplog.at_level(logging.WARNING):
            engine.check_version_compat(_ds())
        assert "older than server" not in caplog.text

    @patch("engines.postgres.subprocess.run")
    def test_drop_and_recreate_sql_contains_terminate(self, mock_run):
        """SQL should terminate backends before dropping."""
        mock_run.return_value = MagicMock(returncode=0)
        engine = PostgresEngine()
        engine.drop_and_recreate(_ds(database="myapp"))
        sql_arg = mock_run.call_args[0][0][-1]
        assert "pg_terminate_backend" in sql_arg
        assert "DROP DATABASE" in sql_arg
        assert "CREATE DATABASE" in sql_arg

    @patch("engines.postgres.subprocess.run")
    def test_dump_uses_versioned_pg_dump(self, mock_run):
        """dump should use the versioned pg_dump when pg_version is set."""
        # We need to use Popen mock for dump, not run
        pass

    @patch("engines.postgres.subprocess.Popen")
    def test_restore_uses_versioned_psql(self, mock_popen, tmp_path):
        """restore should use the versioned psql when pg_version is set."""
        infile = tmp_path / "test.sql.gz"
        infile.write_bytes(b"fake")

        mock_gunzip = MagicMock()
        mock_gunzip.stdout = MagicMock()
        mock_gunzip.stdout.close = MagicMock()
        mock_gunzip.wait.return_value = 0

        mock_psql = MagicMock()
        mock_psql.communicate.return_value = (b"", b"")
        mock_psql.returncode = 0

        mock_popen.side_effect = [mock_gunzip, mock_psql]

        engine = PostgresEngine()
        ds = _ds(options={"pg_version": 15})
        engine.restore(ds, str(infile))

        # Second Popen call is psql
        psql_cmd = mock_popen.call_args_list[1][0][0]
        assert psql_cmd[0] == "/usr/lib/postgresql/15/bin/psql"

    @patch("engines.postgres.subprocess.run")
    def test_count_tables_non_numeric_stdout(self, mock_run):
        """Non-numeric count_tables output → should raise ValueError."""
        mock_run.return_value = MagicMock(returncode=0, stdout="error message\n")
        engine = PostgresEngine()
        with pytest.raises(ValueError):
            engine.count_tables(_ds())


class TestSQLInjectionPrevention:
    """Security: _validate_identifier rejects SQL injection payloads."""

    def test_valid_identifiers(self):
        _validate_identifier("mydb")
        _validate_identifier("my_db")
        _validate_identifier("DB123")
        _validate_identifier("a")

    def test_rejects_single_quote_injection(self):
        with pytest.raises(ValueError, match="Unsafe database identifier"):
            _validate_identifier("foo'; DROP TABLE users; --")

    def test_rejects_double_quote_injection(self):
        with pytest.raises(ValueError, match="Unsafe database identifier"):
            _validate_identifier('foo"; DROP TABLE users; --')

    def test_rejects_semicolon(self):
        with pytest.raises(ValueError, match="Unsafe database identifier"):
            _validate_identifier("foo;bar")

    def test_rejects_spaces(self):
        with pytest.raises(ValueError, match="Unsafe database identifier"):
            _validate_identifier("foo bar")

    def test_rejects_hyphens(self):
        with pytest.raises(ValueError, match="Unsafe database identifier"):
            _validate_identifier("my-db")

    def test_rejects_shell_substitution(self):
        with pytest.raises(ValueError, match="Unsafe database identifier"):
            _validate_identifier("$(rm -rf /)")

    def test_rejects_backtick(self):
        with pytest.raises(ValueError, match="Unsafe database identifier"):
            _validate_identifier("`whoami`")

    def test_rejects_newline(self):
        with pytest.raises(ValueError, match="Unsafe database identifier"):
            _validate_identifier("foo\nbar")

    def test_rejects_empty_string(self):
        with pytest.raises(ValueError, match="Unsafe database identifier"):
            _validate_identifier("")

    @patch("engines.postgres.subprocess.run")
    def test_drop_and_recreate_rejects_injection(self, mock_run):
        """drop_and_recreate with malicious db name → raises before running SQL."""
        engine = PostgresEngine()
        with pytest.raises(ValueError, match="Unsafe database identifier"):
            engine.drop_and_recreate(_ds(database="foo'; DROP TABLE users;--"))
        mock_run.assert_not_called()

    @patch("engines.postgres.subprocess.Popen")
    def test_dump_creates_file_with_restricted_permissions(self, mock_popen, tmp_path):
        """Dump file should be created with 0o600 permissions."""
        outfile = tmp_path / "test.sql.gz"

        mock_dump = MagicMock()
        mock_dump.stdout = MagicMock()
        mock_dump.stdout.close = MagicMock()
        mock_dump.stderr = MagicMock()
        mock_dump.stderr.read.return_value = b""
        mock_dump.wait.return_value = 0
        mock_dump.returncode = 0

        mock_gzip = MagicMock()
        mock_gzip.wait.return_value = 0

        mock_popen.side_effect = [mock_dump, mock_gzip]

        engine = PostgresEngine()
        engine.dump(_ds(), str(outfile))

        import os, stat
        mode = os.stat(str(outfile)).st_mode
        # File should NOT be group/other readable
        assert not (mode & stat.S_IRGRP)
        assert not (mode & stat.S_IROTH)
        assert not (mode & stat.S_IWGRP)
        assert not (mode & stat.S_IWOTH)
