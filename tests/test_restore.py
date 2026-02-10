"""Tests for restore module."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from config import Datasource
from restore import list_backups, run_restore
from stores import BackupInfo


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


def _bi(key: str, ts: datetime, size: int = 1000) -> BackupInfo:
    return BackupInfo(key=key, filename=key.rsplit("/", 1)[-1], timestamp=ts, size=size)


class TestListBackups:
    def test_prints_backups(self, capsys):
        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260101-120000.sql.gz",
                 datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc), size=2048),
        ]
        list_backups(store, "prod", "testdb")
        out = capsys.readouterr().out
        assert "2026-01-01 12:00:00" in out
        assert "2.0 KB" in out
        assert "Total: 1 backup(s)" in out

    def test_no_backups(self, capsys):
        store = MagicMock()
        store.list.return_value = []
        list_backups(store, "prod", "testdb")
        out = capsys.readouterr().out
        assert "No backups found" in out

    def test_size_formatting_bytes(self, capsys):
        store = MagicMock()
        store.list.return_value = [
            _bi("k", datetime(2026, 1, 1, tzinfo=timezone.utc), size=500),
        ]
        list_backups(store, "", "db")
        assert "500 B" in capsys.readouterr().out

    def test_size_formatting_mb(self, capsys):
        store = MagicMock()
        store.list.return_value = [
            _bi("k", datetime(2026, 1, 1, tzinfo=timezone.utc), size=5 * 1024 * 1024),
        ]
        list_backups(store, "", "db")
        assert "5.0 MB" in capsys.readouterr().out

    def test_size_formatting_gb(self, capsys):
        store = MagicMock()
        store.list.return_value = [
            _bi("k", datetime(2026, 1, 1, tzinfo=timezone.utc), size=2 * 1024 ** 3),
        ]
        list_backups(store, "", "db")
        assert "2.0 GB" in capsys.readouterr().out


class TestRunRestore:
    @patch("restore.create_engine")
    def test_restore_latest(self, mock_create_engine, tmp_path):
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 0
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260101-120000.sql.gz",
                 datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]

        # download creates the file
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        run_restore(_ds(), store, "prod")

        # Should pick the latest (last in sorted-oldest-first list)
        # First download is the backup, second is the .sha256 sidecar
        downloaded_key = store.download.call_args_list[0][0][0]
        assert "20260102" in downloaded_key
        mock_engine.restore.assert_called_once()

    @patch("restore.create_engine")
    def test_restore_specific_filename(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 0
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260101-120000.sql.gz",
                 datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]

        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        run_restore(_ds(), store, "prod", filename="db-20260101-120000.sql.gz")

        downloaded_key = store.download.call_args[0][0]
        assert "20260101" in downloaded_key

    @patch("restore.create_engine")
    def test_no_backups_exits(self, mock_create_engine):
        mock_create_engine.return_value = MagicMock()
        store = MagicMock()
        store.list.return_value = []
        with pytest.raises(SystemExit):
            run_restore(_ds(), store, "prod")

    @patch("restore.create_engine")
    def test_filename_not_found_exits(self, mock_create_engine):
        mock_create_engine.return_value = MagicMock()
        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260101-120000.sql.gz",
                 datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        with pytest.raises(SystemExit):
            run_restore(_ds(), store, "prod", filename="nonexistent.sql.gz")

    @patch("restore.create_engine")
    def test_existing_tables_auto_confirm(self, mock_create_engine):
        """With tables and --auto-confirm, drops and restores."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 15
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        run_restore(_ds(), store, "prod", auto_confirm=True)

        mock_engine.drop_and_recreate.assert_called_once()
        mock_engine.restore.assert_called_once()

    @patch("restore.create_engine")
    @patch("builtins.input", return_value="n")
    def test_existing_tables_user_declines_exits(self, mock_input, mock_create_engine):
        """With tables and user says no, exits."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 5
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        with pytest.raises(SystemExit):
            run_restore(_ds(), store, "prod")

        mock_engine.drop_and_recreate.assert_not_called()
        mock_engine.restore.assert_not_called()

    @patch("restore.create_engine")
    @patch("builtins.input", return_value="y")
    def test_existing_tables_user_confirms(self, mock_input, mock_create_engine):
        """With tables and user says yes, drops and restores."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 5
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        run_restore(_ds(), store, "prod")

        mock_engine.drop_and_recreate.assert_called_once()
        mock_engine.restore.assert_called_once()

    @patch("restore.create_engine")
    def test_engine_restore_failure_propagates(self, mock_create_engine):
        """If engine.restore() fails, error propagates."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 0
        mock_engine.restore.side_effect = RuntimeError("psql failed")
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        with pytest.raises(RuntimeError, match="psql failed"):
            run_restore(_ds(), store, "prod")

    @patch("restore.create_engine")
    def test_drop_recreate_failure_propagates(self, mock_create_engine):
        """If drop_and_recreate fails, error propagates after download+verify."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 10
        mock_engine.drop_and_recreate.side_effect = RuntimeError("permission denied")
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        with pytest.raises(RuntimeError, match="permission denied"):
            run_restore(_ds(), store, "prod", auto_confirm=True)

        assert store.download.call_count >= 1  # backup + possibly sidecar
        mock_engine.verify.assert_called_once()
        mock_engine.restore.assert_not_called()

    @patch("restore.create_engine")
    def test_empty_database_skips_drop(self, mock_create_engine):
        """Zero tables → no drop, just restore."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 0
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        run_restore(_ds(), store, "prod")

        mock_engine.drop_and_recreate.assert_not_called()
        mock_engine.restore.assert_called_once()


class TestListBackupsEdgeCases:
    def test_size_exactly_1kb(self, capsys):
        store = MagicMock()
        store.list.return_value = [
            _bi("k", datetime(2026, 1, 1, tzinfo=timezone.utc), size=1024),
        ]
        list_backups(store, "", "db")
        assert "1.0 KB" in capsys.readouterr().out

    def test_size_exactly_1mb(self, capsys):
        store = MagicMock()
        store.list.return_value = [
            _bi("k", datetime(2026, 1, 1, tzinfo=timezone.utc), size=1024 * 1024),
        ]
        list_backups(store, "", "db")
        assert "1.0 MB" in capsys.readouterr().out

    def test_size_exactly_1gb(self, capsys):
        store = MagicMock()
        store.list.return_value = [
            _bi("k", datetime(2026, 1, 1, tzinfo=timezone.utc), size=1024 ** 3),
        ]
        list_backups(store, "", "db")
        assert "1.0 GB" in capsys.readouterr().out

    def test_size_zero_bytes(self, capsys):
        store = MagicMock()
        store.list.return_value = [
            _bi("k", datetime(2026, 1, 1, tzinfo=timezone.utc), size=0),
        ]
        list_backups(store, "", "db")
        assert "0 B" in capsys.readouterr().out

    def test_multiple_backups_sorted(self, capsys):
        """List displays multiple backups and total count."""
        store = MagicMock()
        store.list.return_value = [
            _bi("a", datetime(2026, 1, 1, tzinfo=timezone.utc), size=100),
            _bi("b", datetime(2026, 1, 2, tzinfo=timezone.utc), size=200),
            _bi("c", datetime(2026, 1, 3, tzinfo=timezone.utc), size=300),
        ]
        list_backups(store, "", "db")
        out = capsys.readouterr().out
        assert "Total: 3 backup(s)" in out


class TestRunRestoreEdgeCases:
    @patch("restore.create_engine")
    @patch("builtins.input", return_value="YES")
    def test_user_input_yes_uppercase_accepted(self, mock_input, mock_create_engine):
        """'YES' (uppercase) → .lower() converts to 'yes' → accepted."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 5
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        run_restore(_ds(), store, "prod")
        mock_engine.drop_and_recreate.assert_called_once()

    @patch("restore.create_engine")
    @patch("builtins.input", return_value="nah")
    def test_user_input_other_text_exits(self, mock_input, mock_create_engine):
        """Any text besides 'y'/'yes' → exits."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 5
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        with pytest.raises(SystemExit):
            run_restore(_ds(), store, "prod")

    @patch("restore.create_engine")
    @patch("builtins.input", return_value="yes")
    def test_user_input_yes_lowercase_accepted(self, mock_input, mock_create_engine):
        """'yes' (lowercase) should be accepted."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 5
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        run_restore(_ds(), store, "prod")
        mock_engine.drop_and_recreate.assert_called_once()
        mock_engine.restore.assert_called_once()

    @patch("restore.create_engine")
    @patch("builtins.input", return_value="")
    def test_user_input_empty_exits(self, mock_input, mock_create_engine):
        """Empty input → should exit (not in 'y', 'yes')."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 5
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        with pytest.raises(SystemExit):
            run_restore(_ds(), store, "prod")

    @patch("restore.create_engine")
    def test_download_failure_prevents_drop(self, mock_create_engine):
        """If download fails, database is NOT dropped (download happens first)."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 5
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        store.download.side_effect = RuntimeError("network error")

        with pytest.raises(RuntimeError, match="network error"):
            run_restore(_ds(), store, "prod", auto_confirm=True)

        # Drop should NOT have been called (download failed first)
        mock_engine.drop_and_recreate.assert_not_called()
        mock_engine.restore.assert_not_called()


class TestRestoreVerification:
    @patch("restore.create_engine")
    def test_verify_called_before_restore(self, mock_create_engine):
        """engine.verify called after download and before engine.restore."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 0
        mock_create_engine.return_value = mock_engine

        call_order = []
        mock_engine.verify.side_effect = lambda ds, path: call_order.append("verify")
        mock_engine.restore.side_effect = lambda ds, path: call_order.append("restore")

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            call_order.append("download")
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        run_restore(_ds(), store, "prod")

        # download → verify → download (sidecar) → restore
        assert call_order[0] == "download"
        assert call_order[1] == "verify"
        assert call_order[-1] == "restore"

    @patch("restore.create_engine")
    def test_verify_failure_prevents_restore(self, mock_create_engine):
        """engine.verify raises → engine.restore never called."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 0
        mock_engine.verify.side_effect = RuntimeError("corrupt backup")
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        with pytest.raises(RuntimeError, match="corrupt backup"):
            run_restore(_ds(), store, "prod")

        mock_engine.restore.assert_not_called()

    @patch("restore.create_engine")
    def test_verify_failure_prevents_drop(self, mock_create_engine):
        """Verify fails → DB is NOT dropped (verify happens before drop)."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 10
        mock_engine.verify.side_effect = RuntimeError("corrupt backup")
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]
        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        with pytest.raises(RuntimeError, match="corrupt backup"):
            run_restore(_ds(), store, "prod", auto_confirm=True)

        mock_engine.drop_and_recreate.assert_not_called()
        mock_engine.restore.assert_not_called()


class TestRestoreChecksum:
    @patch("restore.create_engine")
    def test_checksum_verified_when_sidecar_present(self, mock_create_engine):
        """Valid .sha256 sidecar → checksum verified, restore proceeds."""
        import hashlib

        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 0
        mock_create_engine.return_value = mock_engine

        backup_data = b"backup content"
        expected_hash = hashlib.sha256(backup_data).hexdigest()

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]

        def fake_download(key, path):
            if key.endswith(".sha256"):
                with open(path, "w") as f:
                    f.write(expected_hash)
            else:
                with open(path, "wb") as f:
                    f.write(backup_data)

        store.download.side_effect = fake_download

        run_restore(_ds(), store, "prod")
        mock_engine.restore.assert_called_once()

    @patch("restore.create_engine")
    def test_checksum_mismatch_raises(self, mock_create_engine):
        """Checksum mismatch → RuntimeError, restore aborted."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 0
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]

        def fake_download(key, path):
            if key.endswith(".sha256"):
                with open(path, "w") as f:
                    f.write("a" * 64)  # wrong hash
            else:
                with open(path, "wb") as f:
                    f.write(b"backup content")

        store.download.side_effect = fake_download

        with pytest.raises(RuntimeError, match="Checksum mismatch"):
            run_restore(_ds(), store, "prod")

        mock_engine.restore.assert_not_called()

    @patch("restore.create_engine")
    def test_missing_sidecar_gracefully_skipped(self, mock_create_engine):
        """No .sha256 sidecar → restore proceeds (backwards compatible)."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 0
        mock_create_engine.return_value = mock_engine

        store = MagicMock()
        store.list.return_value = [
            _bi("prod/testdb/db-20260102-120000.sql.gz",
                 datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)),
        ]

        def fake_download(key, path):
            if key.endswith(".sha256"):
                raise RuntimeError("not found")
            with open(path, "wb") as f:
                f.write(b"backup content")

        store.download.side_effect = fake_download

        run_restore(_ds(), store, "prod")
        mock_engine.restore.assert_called_once()
