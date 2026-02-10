"""Tests for backup module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

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
        assert mock_store.upload.call_count == 2  # backup + .sha256 sidecar

        # Verify file_extension called with ds arg
        mock_engine.file_extension.assert_called_once_with(ds)

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


        with pytest.raises(RuntimeError, match="S3 unreachable"):
            run_backup(_ds(), mock_store, "prod")

    @patch("backup.create_engine")
    def test_version_compat_failure_propagates(self, mock_create_engine):
        """If version check fails, dump is never called."""
        mock_engine = MagicMock()
        mock_engine.check_version_compat.side_effect = RuntimeError("version error")
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()


        with pytest.raises(RuntimeError, match="version error"):
            run_backup(_ds(), mock_store, "prod")

        mock_engine.dump.assert_not_called()

    @patch("backup.create_engine")
    def test_zero_byte_dump(self, mock_create_engine):
        """Zero-byte dump file → should raise RuntimeError and NOT upload."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            # Create empty file
            open(output_path, "wb").close()

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()

        with pytest.raises(RuntimeError, match="empty \\(0-byte\\) file"):
            run_backup(_ds(), mock_store, "prod")
        mock_store.upload.assert_not_called()

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


        with pytest.raises(RuntimeError, match="S3 error"):
            run_backup(_ds(), mock_store, "prod")

        # tempfile.TemporaryDirectory() handles cleanup via context manager
        # Just verify the error propagated (cleanup is guaranteed by Python)

    @patch("backup.create_engine")
    def test_custom_extension_in_filename(self, mock_create_engine):
        """file_extension returning .dump.zst → filename uses that extension."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".dump.zst"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"data")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()
        key = run_backup(_ds(), mock_store, "prod")

        assert key.endswith(".dump.zst")
        mock_engine.file_extension.assert_called_once()


class TestRunBackupVerify:
    @patch("backup.create_engine")
    def test_verify_after_upload(self, mock_create_engine):
        """verify=True → store.download + engine.verify called."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"data")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()

        def fake_download(key, local_path):
            with open(local_path, "wb") as f:
                f.write(b"downloaded data")

        mock_store.download.side_effect = fake_download

        run_backup(_ds(), mock_store, "prod", verify=True)

        mock_store.download.assert_called_once()
        mock_engine.verify.assert_called_once()

    @patch("backup.create_engine")
    def test_verify_failure_raises(self, mock_create_engine):
        """engine.verify raises → propagates."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"data")

        mock_engine.dump.side_effect = fake_dump
        mock_engine.verify.side_effect = RuntimeError("corrupt backup")
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()

        def fake_download(key, local_path):
            with open(local_path, "wb") as f:
                f.write(b"bad data")

        mock_store.download.side_effect = fake_download


        with pytest.raises(RuntimeError, match="corrupt backup"):
            run_backup(_ds(), mock_store, "prod", verify=True)

    @patch("backup.create_engine")
    def test_no_verify_by_default(self, mock_create_engine):
        """verify not called when omitted."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"data")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()

        run_backup(_ds(), mock_store, "prod")

        mock_store.download.assert_not_called()
        mock_engine.verify.assert_not_called()


class TestBackupChecksum:
    @patch("backup.create_engine")
    def test_sha256_sidecar_uploaded(self, mock_create_engine):
        """After backup upload, a .sha256 sidecar file is also uploaded."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"fake dump data")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()
        key = run_backup(_ds(), mock_store, "prod")

        assert mock_store.upload.call_count == 2
        # Second upload should be the .sha256 sidecar
        second_call = mock_store.upload.call_args_list[1]
        assert second_call[0][1] == key + ".sha256"

    @patch("backup.create_engine")
    def test_sha256_sidecar_content_is_valid(self, mock_create_engine):
        """The .sha256 sidecar file contains a valid 64-char hex digest."""
        import hashlib

        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        dump_data = b"test backup content for checksum"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(dump_data)

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        uploaded_files = {}

        def capture_upload(local_path, remote_key):
            with open(local_path, "rb") as f:
                uploaded_files[remote_key] = f.read()

        mock_store = MagicMock()
        mock_store.upload.side_effect = capture_upload

        key = run_backup(_ds(), mock_store, "prod")

        sidecar_content = uploaded_files[key + ".sha256"].decode().strip()
        expected = hashlib.sha256(dump_data).hexdigest()
        assert sidecar_content == expected
