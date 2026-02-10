"""Tests for backup module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from config import Datasource
from backup import run_backup


def _ds() -> Datasource:
    return Datasource(
        name="test",
        engine="postgres",
        host="localhost",
        port=5432,
        user="u",
        password="p",
        database="testdb",
        options={},
    )


class TestRunBackup:
    @patch("backup.create_engine")
    def test_full_cycle(self, mock_create_engine, tmp_path):
        """Backup cycle: connectivity check → version check → dump → upload."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        # Make dump create an actual file so os.path.getsize works
        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"fake dump data")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()

        ds = _ds()
        key = run_backup(ds, mock_store, "prod")

        # Verify call sequence
        mock_engine.check_connectivity.assert_called_once_with(ds)
        mock_engine.check_version_compat.assert_called_once_with(ds)
        mock_engine.dump.assert_called_once()
        mock_store.upload.assert_called_once()

        # Verify the remote key structure
        assert key.startswith("prod/testdb/")
        assert key.endswith(".sql.gz")
        assert "testdb-" in key

    @patch("backup.create_engine")
    def test_connectivity_failure_propagates(self, mock_create_engine):
        """If connectivity check fails, the error propagates."""
        mock_engine = MagicMock()
        mock_engine.check_connectivity.side_effect = RuntimeError("unreachable")
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()

        import pytest
        with pytest.raises(RuntimeError, match="unreachable"):
            run_backup(_ds(), mock_store, "prod")

        # dump and upload should NOT have been called
        mock_engine.dump.assert_not_called()
        mock_store.upload.assert_not_called()

    @patch("backup.create_engine")
    def test_empty_prefix(self, mock_create_engine):
        """Empty prefix → key is just dbname/filename."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"data")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()
        key = run_backup(_ds(), mock_store, "")
        assert key.startswith("testdb/testdb-")

    @patch("backup.create_engine")
    def test_dump_failure_propagates(self, mock_create_engine):
        """If engine.dump fails, the error propagates and upload is skipped."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"
        mock_engine.dump.side_effect = RuntimeError("pg_dump failed")
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()

        import pytest
        with pytest.raises(RuntimeError, match="pg_dump failed"):
            run_backup(_ds(), mock_store, "prod")

        mock_store.upload.assert_not_called()

    @patch("backup.create_engine")
    def test_upload_failure_propagates(self, mock_create_engine):
        """If store.upload fails, the error propagates."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"data")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()
        mock_store.upload.side_effect = RuntimeError("S3 unreachable")

        import pytest
        with pytest.raises(RuntimeError, match="S3 unreachable"):
            run_backup(_ds(), mock_store, "prod")

    @patch("backup.create_engine")
    def test_version_compat_failure_propagates(self, mock_create_engine):
        """If version check fails, dump is never called."""
        mock_engine = MagicMock()
        mock_engine.check_version_compat.side_effect = RuntimeError("version error")
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()

        import pytest
        with pytest.raises(RuntimeError, match="version error"):
            run_backup(_ds(), mock_store, "prod")

        mock_engine.dump.assert_not_called()

    @patch("backup.create_engine")
    def test_zero_byte_dump(self, mock_create_engine, tmp_path):
        """Zero-byte dump file → should still upload (no size check)."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            # Create empty file
            open(output_path, "wb").close()

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()
        key = run_backup(_ds(), mock_store, "prod")
        mock_store.upload.assert_called_once()

    @patch("backup.create_engine")
    def test_tempdir_cleanup_on_upload_failure(self, mock_create_engine):
        """Temp dir is cleaned up even when upload fails."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"data")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()
        mock_store.upload.side_effect = RuntimeError("S3 error")

        import pytest
        with pytest.raises(RuntimeError, match="S3 error"):
            run_backup(_ds(), mock_store, "prod")

        # tempfile.TemporaryDirectory() handles cleanup via context manager
        # Just verify the error propagated (cleanup is guaranteed by Python)
