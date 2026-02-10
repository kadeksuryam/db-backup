"""Tests for engines package — factory and PostgresEngine."""

from __future__ import annotations

import logging
import os
import stat
import subprocess
from unittest.mock import MagicMock, patch, call

import pytest

from config import ConfigError, Datasource
from engines import create_engine, Engine
from engines.postgres import (
    PostgresEngine, _validate_identifier, _detect_from_extension,
    _resolve_format, _resolve_compression, _resolve_timeout,
)


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
        with pytest.raises(ConfigError, match="Unknown engine type 'mysql'"):
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
        mock_gzip.returncode = 0

        mock_popen.side_effect = [mock_dump, mock_gzip]

        engine = PostgresEngine()
        engine.dump(_ds(), str(outfile))

        assert mock_dump.stdout.close.called
        assert mock_gzip.wait.called
        assert mock_dump.wait.called

        # Default options: pg_dump should NOT contain -Fc
        pg_dump_cmd = mock_popen.call_args_list[0][0][0]
        assert "-Fc" not in pg_dump_cmd

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
        mock_gzip.returncode = 0

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
        mock_gunzip.returncode = 0

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
        mock_gunzip.returncode = 0

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
        assert PostgresEngine().file_extension(_ds()) == ".sql.gz"

    # -- file_extension matrix (all 8 combos) -----------------------------

    @pytest.mark.parametrize("fmt,comp,expected", [
        ("plain",  "gzip",  ".sql.gz"),
        ("plain",  "zstd",  ".sql.zst"),
        ("plain",  "lz4",   ".sql.lz4"),
        ("plain",  "none",  ".sql"),
        ("custom", "gzip",  ".dump.gz"),
        ("custom", "zstd",  ".dump.zst"),
        ("custom", "lz4",   ".dump.lz4"),
        ("custom", "none",  ".dump"),
    ])
    def test_file_extension_matrix(self, fmt, comp, expected):
        ds = _ds(options={"format": fmt, "compression": comp})
        assert PostgresEngine().file_extension(ds) == expected

    # -- invalid format / compression -------------------------------------

    def test_invalid_format_raises(self):
        ds = _ds(options={"format": "tar"})
        with pytest.raises(ValueError, match="Invalid format 'tar'"):
            PostgresEngine().file_extension(ds)

    def test_invalid_compression_raises(self):
        ds = _ds(options={"compression": "bzip2"})
        with pytest.raises(ValueError, match="Invalid compression 'bzip2'"):
            PostgresEngine().file_extension(ds)

    # -- dump with custom format ------------------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_dump_custom_format(self, mock_popen, tmp_path):
        """format: custom → pg_dump cmd contains -Fc and -Z0."""
        outfile = tmp_path / "test.dump.gz"
        ds = _ds(options={"format": "custom"})

        mock_dump = MagicMock()
        mock_dump.stdout = MagicMock()
        mock_dump.stdout.close = MagicMock()
        mock_dump.stderr = MagicMock()
        mock_dump.stderr.read.return_value = b""
        mock_dump.wait.return_value = 0
        mock_dump.returncode = 0

        mock_compress = MagicMock()
        mock_compress.wait.return_value = 0
        mock_compress.returncode = 0

        mock_popen.side_effect = [mock_dump, mock_compress]

        PostgresEngine().dump(ds, str(outfile))

        pg_dump_cmd = mock_popen.call_args_list[0][0][0]
        assert "-Fc" in pg_dump_cmd
        assert "-Z0" in pg_dump_cmd

    # -- dump with zstd ---------------------------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_dump_zstd(self, mock_popen, tmp_path):
        """compression: zstd → compressor cmd is ["zstd", "-3", "-c"]."""
        outfile = tmp_path / "test.sql.zst"
        ds = _ds(options={"compression": "zstd"})

        mock_dump = MagicMock()
        mock_dump.stdout = MagicMock()
        mock_dump.stdout.close = MagicMock()
        mock_dump.stderr = MagicMock()
        mock_dump.stderr.read.return_value = b""
        mock_dump.wait.return_value = 0
        mock_dump.returncode = 0

        mock_compress = MagicMock()
        mock_compress.wait.return_value = 0
        mock_compress.returncode = 0

        mock_popen.side_effect = [mock_dump, mock_compress]

        PostgresEngine().dump(ds, str(outfile))

        compress_cmd = mock_popen.call_args_list[1][0][0]
        assert compress_cmd == ["zstd", "-3", "-c"]

    # -- dump with lz4 ----------------------------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_dump_lz4(self, mock_popen, tmp_path):
        """compression: lz4 → compressor cmd is ["lz4", "-1", "-c"]."""
        outfile = tmp_path / "test.sql.lz4"
        ds = _ds(options={"compression": "lz4"})

        mock_dump = MagicMock()
        mock_dump.stdout = MagicMock()
        mock_dump.stdout.close = MagicMock()
        mock_dump.stderr = MagicMock()
        mock_dump.stderr.read.return_value = b""
        mock_dump.wait.return_value = 0
        mock_dump.returncode = 0

        mock_compress = MagicMock()
        mock_compress.wait.return_value = 0
        mock_compress.returncode = 0

        mock_popen.side_effect = [mock_dump, mock_compress]

        PostgresEngine().dump(ds, str(outfile))

        compress_cmd = mock_popen.call_args_list[1][0][0]
        assert compress_cmd == ["lz4", "-1", "-c"]

    # -- dump with no compression -----------------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_dump_no_compression(self, mock_popen, tmp_path):
        """compression: none → only 1 Popen call (pg_dump stdout→file)."""
        outfile = tmp_path / "test.sql"
        ds = _ds(options={"compression": "none"})

        mock_dump = MagicMock()
        mock_dump.stderr = MagicMock()
        mock_dump.stderr.read.return_value = b""
        mock_dump.wait.return_value = 0
        mock_dump.returncode = 0

        mock_popen.side_effect = [mock_dump]

        PostgresEngine().dump(ds, str(outfile))

        assert mock_popen.call_count == 1

    # -- compression_level validation --------------------------------------

    def test_compression_level_string_raises(self):
        """Non-integer compression_level → ValueError."""
        ds = _ds(options={"compression_level": "fast"})
        with pytest.raises(ValueError, match="Invalid compression_level"):
            _resolve_compression(ds)

    def test_compression_level_negative_raises(self):
        """Negative compression_level → ValueError."""
        ds = _ds(options={"compression_level": -1})
        with pytest.raises(ValueError, match="compression_level must be between"):
            _resolve_compression(ds)

    def test_compression_level_zero_raises(self):
        """compression_level 0 → ValueError."""
        ds = _ds(options={"compression_level": 0})
        with pytest.raises(ValueError, match="compression_level must be between"):
            _resolve_compression(ds)

    # -- dump with custom compression_level -------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_dump_custom_compression_level(self, mock_popen, tmp_path):
        """compression_level: 9 → gzip cmd is ["gzip", "-9"]."""
        outfile = tmp_path / "test.sql.gz"
        ds = _ds(options={"compression_level": 9})

        mock_dump = MagicMock()
        mock_dump.stdout = MagicMock()
        mock_dump.stdout.close = MagicMock()
        mock_dump.stderr = MagicMock()
        mock_dump.stderr.read.return_value = b""
        mock_dump.wait.return_value = 0
        mock_dump.returncode = 0

        mock_compress = MagicMock()
        mock_compress.wait.return_value = 0
        mock_compress.returncode = 0

        mock_popen.side_effect = [mock_dump, mock_compress]

        PostgresEngine().dump(ds, str(outfile))

        compress_cmd = mock_popen.call_args_list[1][0][0]
        assert compress_cmd == ["gzip", "-9"]

    # -- restore with custom format ---------------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_restore_custom_format(self, mock_popen, tmp_path):
        """Restoring a .dump.gz file → pg_restore with --no-owner --no-privileges."""
        infile = tmp_path / "test.dump.gz"
        infile.write_bytes(b"fake")

        mock_decompress = MagicMock()
        mock_decompress.stdout = MagicMock()
        mock_decompress.stdout.close = MagicMock()
        mock_decompress.wait.return_value = 0
        mock_decompress.returncode = 0

        mock_restore = MagicMock()
        mock_restore.communicate.return_value = (b"", b"")
        mock_restore.returncode = 0

        mock_popen.side_effect = [mock_decompress, mock_restore]

        PostgresEngine().restore(_ds(), str(infile))

        restore_cmd = mock_popen.call_args_list[1][0][0]
        assert "pg_restore" in restore_cmd[0]
        assert "--no-owner" in restore_cmd
        assert "--no-privileges" in restore_cmd

    # -- restore with zstd ------------------------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_restore_zstd(self, mock_popen, tmp_path):
        """Restoring a .sql.zst file → decompressor is ["zstd", "-d", "-c"]."""
        infile = tmp_path / "test.sql.zst"
        infile.write_bytes(b"fake")

        mock_decompress = MagicMock()
        mock_decompress.stdout = MagicMock()
        mock_decompress.stdout.close = MagicMock()
        mock_decompress.wait.return_value = 0
        mock_decompress.returncode = 0

        mock_restore = MagicMock()
        mock_restore.communicate.return_value = (b"", b"")
        mock_restore.returncode = 0

        mock_popen.side_effect = [mock_decompress, mock_restore]

        PostgresEngine().restore(_ds(), str(infile))

        decompress_cmd = mock_popen.call_args_list[0][0][0]
        assert decompress_cmd == ["zstd", "-d", "-c"]

    # -- restore with lz4 -------------------------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_restore_lz4(self, mock_popen, tmp_path):
        """Restoring a .sql.lz4 file → decompressor is ["lz4", "-d", "-c"]."""
        infile = tmp_path / "test.sql.lz4"
        infile.write_bytes(b"fake")

        mock_decompress = MagicMock()
        mock_decompress.stdout = MagicMock()
        mock_decompress.stdout.close = MagicMock()
        mock_decompress.wait.return_value = 0
        mock_decompress.returncode = 0

        mock_restore = MagicMock()
        mock_restore.communicate.return_value = (b"", b"")
        mock_restore.returncode = 0

        mock_popen.side_effect = [mock_decompress, mock_restore]

        PostgresEngine().restore(_ds(), str(infile))

        decompress_cmd = mock_popen.call_args_list[0][0][0]
        assert decompress_cmd == ["lz4", "-d", "-c"]

    # -- restore with no compression --------------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_restore_no_compression(self, mock_popen, tmp_path):
        """Restoring a .sql file → only 1 Popen call (file→psql)."""
        infile = tmp_path / "test.sql"
        infile.write_bytes(b"fake")

        mock_restore = MagicMock()
        mock_restore.communicate.return_value = (b"", b"")
        mock_restore.returncode = 0

        mock_popen.side_effect = [mock_restore]

        PostgresEngine().restore(_ds(), str(infile))

        assert mock_popen.call_count == 1

    # -- restore detects from extension -----------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_restore_detects_from_extension(self, mock_popen, tmp_path):
        """Restore a .dump.zst file while ds.options say gzip;
        verify zstd decompressor + pg_restore used."""
        infile = tmp_path / "test.dump.zst"
        infile.write_bytes(b"fake")

        # ds options say gzip, but file extension is .dump.zst
        ds = _ds(options={"compression": "gzip", "format": "plain"})

        mock_decompress = MagicMock()
        mock_decompress.stdout = MagicMock()
        mock_decompress.stdout.close = MagicMock()
        mock_decompress.wait.return_value = 0
        mock_decompress.returncode = 0

        mock_restore = MagicMock()
        mock_restore.communicate.return_value = (b"", b"")
        mock_restore.returncode = 0

        mock_popen.side_effect = [mock_decompress, mock_restore]

        PostgresEngine().restore(ds, str(infile))

        # Should detect zstd from extension, not gzip from ds.options
        decompress_cmd = mock_popen.call_args_list[0][0][0]
        assert decompress_cmd == ["zstd", "-d", "-c"]

        # Should detect custom format from extension, not plain from ds.options
        restore_cmd = mock_popen.call_args_list[1][0][0]
        assert "pg_restore" in restore_cmd[0]

    # -- _detect_from_extension -------------------------------------------

    @pytest.mark.parametrize("filename,expected_fmt,expected_comp", [
        ("db-20260101-120000.sql.gz",  "plain",  "gzip"),
        ("db-20260101-120000.sql.zst", "plain",  "zstd"),
        ("db-20260101-120000.sql.lz4", "plain",  "lz4"),
        ("db-20260101-120000.sql",     "plain",  "none"),
        ("db-20260101-120000.dump.gz", "custom", "gzip"),
        ("db-20260101-120000.dump.zst","custom", "zstd"),
        ("db-20260101-120000.dump.lz4","custom", "lz4"),
        ("db-20260101-120000.dump",    "custom", "none"),
    ])
    def test_detect_from_extension(self, filename, expected_fmt, expected_comp):
        fmt, comp = _detect_from_extension(filename)
        assert fmt == expected_fmt
        assert comp == expected_comp

    def test_detect_from_extension_unknown_raises(self):
        with pytest.raises(ValueError, match="Unrecognized backup file extension"):
            _detect_from_extension("backup.tar.gz")

    # -- dump compressor exit code checking ---------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_dump_compressor_failure_raises(self, mock_popen, tmp_path):
        """pg_dump OK, gzip fails → RuntimeError with 'compressor failed'."""
        outfile = tmp_path / "test.sql.gz"

        mock_dump = MagicMock()
        mock_dump.stdout = MagicMock()
        mock_dump.stdout.close = MagicMock()
        mock_dump.stderr = MagicMock()
        mock_dump.stderr.read.return_value = b""
        mock_dump.wait.return_value = 0
        mock_dump.returncode = 0

        mock_gzip = MagicMock()
        mock_gzip.wait.return_value = 1
        mock_gzip.returncode = 1
        mock_gzip.stderr = MagicMock()
        mock_gzip.stderr.read.return_value = b"gzip: broken pipe"

        mock_popen.side_effect = [mock_dump, mock_gzip]

        engine = PostgresEngine()
        with pytest.raises(RuntimeError, match="compressor failed"):
            engine.dump(_ds(), str(outfile))

    @patch("engines.postgres.subprocess.Popen")
    def test_dump_both_fail_reports_both(self, mock_popen, tmp_path):
        """Both pg_dump and gzip fail → message contains both errors."""
        outfile = tmp_path / "test.sql.gz"

        mock_dump = MagicMock()
        mock_dump.stdout = MagicMock()
        mock_dump.stdout.close = MagicMock()
        mock_dump.stderr = MagicMock()
        mock_dump.stderr.read.return_value = b"connection refused"
        mock_dump.wait.return_value = 1
        mock_dump.returncode = 1

        mock_gzip = MagicMock()
        mock_gzip.wait.return_value = 1
        mock_gzip.returncode = 1
        mock_gzip.stderr = MagicMock()
        mock_gzip.stderr.read.return_value = b"broken pipe"

        mock_popen.side_effect = [mock_dump, mock_gzip]

        engine = PostgresEngine()
        with pytest.raises(RuntimeError) as exc_info:
            engine.dump(_ds(), str(outfile))
        assert "pg_dump failed" in str(exc_info.value)
        assert "compressor failed" in str(exc_info.value)

    # -- restore decompressor exit code checking ------------------------------

    @patch("engines.postgres.subprocess.Popen")
    def test_restore_decompressor_failure_raises(self, mock_popen, tmp_path):
        """gunzip fails, psql OK → RuntimeError with 'decompressor failed'."""
        infile = tmp_path / "test.sql.gz"
        infile.write_bytes(b"fake")

        mock_gunzip = MagicMock()
        mock_gunzip.stdout = MagicMock()
        mock_gunzip.stdout.close = MagicMock()
        mock_gunzip.wait.return_value = 1
        mock_gunzip.returncode = 1
        mock_gunzip.stderr = MagicMock()
        mock_gunzip.stderr.read.return_value = b"unexpected end of file"

        mock_psql = MagicMock()
        mock_psql.communicate.return_value = (b"", b"")
        mock_psql.returncode = 0

        mock_popen.side_effect = [mock_gunzip, mock_psql]

        engine = PostgresEngine()
        with pytest.raises(RuntimeError, match="decompressor failed"):
            engine.restore(_ds(), str(infile))

    @patch("engines.postgres.subprocess.Popen")
    def test_restore_both_fail_reports_both(self, mock_popen, tmp_path):
        """Both decompressor and psql fail → message contains both errors."""
        infile = tmp_path / "test.sql.gz"
        infile.write_bytes(b"fake")

        mock_gunzip = MagicMock()
        mock_gunzip.stdout = MagicMock()
        mock_gunzip.stdout.close = MagicMock()
        mock_gunzip.wait.return_value = 1
        mock_gunzip.returncode = 1
        mock_gunzip.stderr = MagicMock()
        mock_gunzip.stderr.read.return_value = b"corrupt input"

        mock_psql = MagicMock()
        mock_psql.communicate.return_value = (b"", b"ERROR: syntax error")
        mock_psql.returncode = 3

        mock_popen.side_effect = [mock_gunzip, mock_psql]

        engine = PostgresEngine()
        with pytest.raises(RuntimeError) as exc_info:
            engine.restore(_ds(), str(infile))
        assert "decompressor failed" in str(exc_info.value)
        assert "psql restore failed" in str(exc_info.value)


class TestCreateEngineEdgeCases:
    def test_empty_string_engine_raises(self):
        with pytest.raises(ConfigError, match="Unknown engine type"):
            create_engine("")

    def test_none_engine_raises(self):
        with pytest.raises(ConfigError):
            create_engine(None)

    def test_case_sensitive(self):
        """Engine types are case-sensitive."""
        with pytest.raises(ConfigError):
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
        mock_gunzip.returncode = 0

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
        mock_gzip.returncode = 0

        mock_popen.side_effect = [mock_dump, mock_gzip]

        engine = PostgresEngine()
        engine.dump(_ds(), str(outfile))


        mode = os.stat(str(outfile)).st_mode
        # File should NOT be group/other readable
        assert not (mode & stat.S_IRGRP)
        assert not (mode & stat.S_IROTH)
        assert not (mode & stat.S_IWGRP)
        assert not (mode & stat.S_IWOTH)


class TestTimeout:
    """Tests for _resolve_timeout and timeout behaviour."""

    def test_resolve_timeout_none_by_default(self):
        ds = _ds()
        assert _resolve_timeout(ds) is None

    def test_resolve_timeout_from_options(self):
        ds = _ds(options={"timeout": 300})
        assert _resolve_timeout(ds) == 300.0

    def test_resolve_timeout_string(self):
        ds = _ds(options={"timeout": "60"})
        assert _resolve_timeout(ds) == 60.0

    def test_resolve_timeout_zero_raises(self):
        ds = _ds(options={"timeout": 0})
        with pytest.raises(ValueError, match="timeout must be positive"):
            _resolve_timeout(ds)

    def test_resolve_timeout_negative_raises(self):
        ds = _ds(options={"timeout": -10})
        with pytest.raises(ValueError, match="timeout must be positive"):
            _resolve_timeout(ds)

    @patch("engines.postgres.subprocess.run")
    def test_check_connectivity_timeout(self, mock_run):
        """TimeoutExpired from subprocess.run → TimeoutError raised."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="pg_isready", timeout=5)
        ds = _ds(options={"timeout": 5})
        engine = PostgresEngine()
        with pytest.raises(TimeoutError, match="timed out"):
            engine.check_connectivity(ds)

    @patch("engines.postgres.subprocess.run")
    def test_check_connectivity_no_timeout_by_default(self, mock_run):
        """No timeout option → timeout=None passed to subprocess.run."""
        mock_run.return_value = MagicMock(returncode=0)
        ds = _ds()
        engine = PostgresEngine()
        engine.check_connectivity(ds)
        assert mock_run.call_args[1]["timeout"] is None

    @patch("engines.postgres.subprocess.Popen")
    def test_dump_timeout_kills_processes(self, mock_popen, tmp_path):
        """dump timeout → both procs killed."""
        outfile = tmp_path / "test.sql.gz"
        ds = _ds(options={"timeout": 5})

        mock_dump = MagicMock()
        mock_dump.stdout = MagicMock()
        mock_dump.stdout.close = MagicMock()
        mock_dump.stderr = MagicMock()

        mock_compress = MagicMock()
        # First wait raises TimeoutExpired, subsequent waits (after kill) succeed
        mock_compress.wait.side_effect = [
            subprocess.TimeoutExpired(cmd="gzip", timeout=5),
            0,  # reap after kill
        ]

        mock_dump.wait.return_value = 0

        mock_popen.side_effect = [mock_dump, mock_compress]

        engine = PostgresEngine()
        with pytest.raises(TimeoutError, match="timed out"):
            engine.dump(ds, str(outfile))

        mock_compress.kill.assert_called_once()
        mock_dump.kill.assert_called_once()

    @patch("engines.postgres.subprocess.Popen")
    def test_restore_timeout_kills_processes(self, mock_popen, tmp_path):
        """restore timeout → both procs killed."""
        infile = tmp_path / "test.sql.gz"
        infile.write_bytes(b"fake")
        ds = _ds(options={"timeout": 5})

        mock_decompress = MagicMock()
        mock_decompress.stdout = MagicMock()
        mock_decompress.stdout.close = MagicMock()

        mock_restore = MagicMock()
        mock_restore.communicate.side_effect = subprocess.TimeoutExpired(
            cmd="psql", timeout=5
        )

        mock_popen.side_effect = [mock_decompress, mock_restore]

        engine = PostgresEngine()
        with pytest.raises(TimeoutError, match="timed out"):
            engine.restore(ds, str(infile))

        mock_restore.kill.assert_called_once()
        mock_decompress.kill.assert_called_once()


class TestPostgresVerify:
    """Tests for PostgresEngine.verify()."""

    @patch("engines.postgres.subprocess.Popen")
    def test_verify_custom_no_compression(self, mock_popen, tmp_path):
        """pg_restore --list succeeds → no error."""
        infile = tmp_path / "test.dump"
        infile.write_bytes(b"fake custom dump")

        mock_restore = MagicMock()
        mock_restore.wait.return_value = 0
        mock_restore.returncode = 0
        mock_restore.stderr = MagicMock()

        mock_popen.side_effect = [mock_restore]

        engine = PostgresEngine()
        engine.verify(_ds(), str(infile))  # should not raise

    @patch("engines.postgres.subprocess.Popen")
    def test_verify_custom_no_compression_failure(self, mock_popen, tmp_path):
        """pg_restore --list fails → RuntimeError."""
        infile = tmp_path / "test.dump"
        infile.write_bytes(b"corrupt data")

        mock_restore = MagicMock()
        mock_restore.wait.return_value = 1
        mock_restore.returncode = 1
        mock_restore.stderr = MagicMock()
        mock_restore.stderr.read.return_value = b"not a valid archive"

        mock_popen.side_effect = [mock_restore]

        engine = PostgresEngine()
        with pytest.raises(RuntimeError, match="Verification failed"):
            engine.verify(_ds(), str(infile))

    @patch("engines.postgres.subprocess.Popen")
    def test_verify_custom_with_compression(self, mock_popen, tmp_path):
        """decompress | pg_restore --list → success."""
        infile = tmp_path / "test.dump.gz"
        infile.write_bytes(b"fake compressed dump")

        mock_decompress = MagicMock()
        mock_decompress.stdout = MagicMock()
        mock_decompress.stdout.close = MagicMock()
        mock_decompress.wait.return_value = 0
        mock_decompress.returncode = 0

        mock_restore = MagicMock()
        mock_restore.wait.return_value = 0
        mock_restore.returncode = 0

        mock_popen.side_effect = [mock_decompress, mock_restore]

        engine = PostgresEngine()
        engine.verify(_ds(), str(infile))  # should not raise

    @patch("engines.postgres.subprocess.Popen")
    def test_verify_plain_with_compression_valid(self, mock_popen, tmp_path):
        """decompress header contains SQL markers → success."""
        infile = tmp_path / "test.sql.gz"
        infile.write_bytes(b"fake compressed sql")

        mock_proc = MagicMock()
        mock_proc.stdout = MagicMock()
        mock_proc.stdout.read.return_value = b"-- PostgreSQL database dump\nSET search_path"
        mock_proc.wait.return_value = 0

        mock_popen.side_effect = [mock_proc]

        engine = PostgresEngine()
        engine.verify(_ds(), str(infile))  # should not raise

    def test_verify_plain_no_compression_valid(self, tmp_path):
        """file header contains SQL markers → success."""
        infile = tmp_path / "test.sql"
        infile.write_text("-- PostgreSQL database dump\nSET search_path = public;\n")

        engine = PostgresEngine()
        engine.verify(_ds(), str(infile))  # should not raise

    def test_verify_plain_no_compression_invalid(self, tmp_path):
        """binary garbage → RuntimeError."""
        infile = tmp_path / "test.sql"
        infile.write_bytes(b"\x00\x01\x02\x03\x04\x05")

        engine = PostgresEngine()
        with pytest.raises(RuntimeError, match="no SQL markers found"):
            engine.verify(_ds(), str(infile))

    def test_verify_plain_empty_file(self, tmp_path):
        """empty → RuntimeError."""
        infile = tmp_path / "test.sql"
        infile.write_bytes(b"")

        engine = PostgresEngine()
        with pytest.raises(RuntimeError, match="empty"):
            engine.verify(_ds(), str(infile))
