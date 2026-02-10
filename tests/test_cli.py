"""Tests for dbbackup CLI (dbbackup.py)."""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch, call

import pytest
import yaml

from dbbackup import cmd_backup, cmd_prune, cmd_list, cmd_restore, main


def _write_config(tmp_path, cfg=None):
    """Write a minimal valid config and return the path."""
    if cfg is None:
        cfg = {
            "datasources": {
                "ds1": {
                    "engine": "postgres",
                    "host": "localhost",
                    "port": 5432,
                    "user": "u",
                    "password": "p",
                    "database": "db1",
                }
            },
            "stores": {
                "s1": {"type": "s3", "bucket": "b"}
            },
            "jobs": {
                "job1": {
                    "datasource": "ds1",
                    "store": "s1",
                    "prefix": "prod",
                    "retention": {"keep_last": 3},
                },
                "job2": {
                    "datasource": "ds1",
                    "store": "s1",
                    "prefix": "staging",
                },
            },
        }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(cfg))
    return str(path)


class TestCmdBackup:
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_single_job(self, mock_run_backup, mock_create_store, tmp_path):
        cfg_path = _write_config(tmp_path)
        import config
        raw = config.load(cfg_path)

        args = argparse.Namespace(all=False, job="job1", prune=False)
        mock_create_store.return_value = MagicMock()

        cmd_backup(args, raw)

        mock_run_backup.assert_called_once()

    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_all_jobs(self, mock_run_backup, mock_create_store, tmp_path):
        cfg_path = _write_config(tmp_path)
        import config
        raw = config.load(cfg_path)

        args = argparse.Namespace(all=True, job=None, prune=False)
        mock_create_store.return_value = MagicMock()

        cmd_backup(args, raw)

        # Both job1 and job2
        assert mock_run_backup.call_count == 2

    @patch("dbbackup.apply_retention")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_backup_with_prune(self, mock_run_backup, mock_create_store, mock_apply_retention, tmp_path):
        cfg_path = _write_config(tmp_path)
        import config
        raw = config.load(cfg_path)

        args = argparse.Namespace(all=False, job="job1", prune=True)
        mock_create_store.return_value = MagicMock()

        cmd_backup(args, raw)

        mock_run_backup.assert_called_once()
        mock_apply_retention.assert_called_once()

    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_backup_without_prune(self, mock_run_backup, mock_create_store, tmp_path):
        cfg_path = _write_config(tmp_path)
        import config
        raw = config.load(cfg_path)

        args = argparse.Namespace(all=False, job="job1", prune=False)
        mock_create_store.return_value = MagicMock()

        cmd_backup(args, raw)

        mock_run_backup.assert_called_once()

    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_partial_failure_exits(self, mock_run_backup, mock_create_store, tmp_path):
        """One job fails, others succeed → exit 1 but all attempted."""
        cfg_path = _write_config(tmp_path)
        import config
        raw = config.load(cfg_path)

        call_count = [0]

        def side_effect(*a, **kw):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("boom")

        mock_run_backup.side_effect = side_effect
        mock_create_store.return_value = MagicMock()

        args = argparse.Namespace(all=True, job=None, prune=False)
        with pytest.raises(SystemExit):
            cmd_backup(args, raw)

        # Both jobs were attempted
        assert mock_run_backup.call_count == 2

    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_all_no_jobs_exits(self, mock_run_backup, mock_create_store, tmp_path):
        cfg_path = _write_config(tmp_path, {
            "datasources": {}, "stores": {}, "jobs": {}
        })
        import config
        raw = config.load(cfg_path)

        args = argparse.Namespace(all=True, job=None, prune=False)
        with pytest.raises(SystemExit):
            cmd_backup(args, raw)


class TestCmdPrune:
    @patch("dbbackup.apply_retention")
    @patch("dbbackup.create_store")
    def test_calls_apply_retention(self, mock_create_store, mock_apply_retention, tmp_path):
        cfg_path = _write_config(tmp_path)
        import config
        raw = config.load(cfg_path)

        args = argparse.Namespace(job="job1")
        mock_create_store.return_value = MagicMock()

        cmd_prune(args, raw)
        mock_apply_retention.assert_called_once()


class TestCmdList:
    @patch("dbbackup.list_backups")
    @patch("dbbackup.create_store")
    def test_calls_list_backups(self, mock_create_store, mock_list_backups, tmp_path):
        cfg_path = _write_config(tmp_path)
        import config
        raw = config.load(cfg_path)

        args = argparse.Namespace(job="job1")
        mock_create_store.return_value = MagicMock()

        cmd_list(args, raw)
        mock_list_backups.assert_called_once()


class TestCmdRestore:
    @patch("dbbackup.run_restore")
    @patch("dbbackup.create_store")
    def test_calls_run_restore(self, mock_create_store, mock_run_restore, tmp_path):
        cfg_path = _write_config(tmp_path)
        import config
        raw = config.load(cfg_path)

        args = argparse.Namespace(job="job1", filename=None, auto_confirm=False)
        mock_create_store.return_value = MagicMock()

        cmd_restore(args, raw)
        mock_run_restore.assert_called_once()
        call_kwargs = mock_run_restore.call_args
        assert call_kwargs[1]["filename"] is None
        assert call_kwargs[1]["auto_confirm"] is False

    @patch("dbbackup.run_restore")
    @patch("dbbackup.create_store")
    def test_passes_filename_and_auto_confirm(self, mock_create_store, mock_run_restore, tmp_path):
        cfg_path = _write_config(tmp_path)
        import config
        raw = config.load(cfg_path)

        args = argparse.Namespace(job="job1", filename="backup.sql.gz", auto_confirm=True)
        mock_create_store.return_value = MagicMock()

        cmd_restore(args, raw)
        call_kwargs = mock_run_restore.call_args
        assert call_kwargs[1]["filename"] == "backup.sql.gz"
        assert call_kwargs[1]["auto_confirm"] is True


class TestCmdBackupEdgeCases:
    @patch("dbbackup.apply_retention")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_all_with_prune(self, mock_run_backup, mock_create_store, mock_apply_retention, tmp_path):
        """--all --prune → backup + prune for each job."""
        cfg_path = _write_config(tmp_path)
        import config
        raw = config.load(cfg_path)

        args = argparse.Namespace(all=True, job=None, prune=True)
        mock_create_store.return_value = MagicMock()

        cmd_backup(args, raw)

        assert mock_run_backup.call_count == 2
        assert mock_apply_retention.call_count == 2

    @patch("dbbackup.apply_retention")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_all_with_prune_one_backup_fails(self, mock_run_backup, mock_create_store, mock_apply_retention, tmp_path):
        """--all --prune, one job fails → other still attempted, exit 1."""
        cfg_path = _write_config(tmp_path)
        import config
        raw = config.load(cfg_path)

        call_count = [0]
        def side_effect(*a, **kw):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("boom")

        mock_run_backup.side_effect = side_effect
        mock_create_store.return_value = MagicMock()

        args = argparse.Namespace(all=True, job=None, prune=True)
        with pytest.raises(SystemExit):
            cmd_backup(args, raw)

        assert mock_run_backup.call_count == 2
        # Prune should only be called for the successful job
        assert mock_apply_retention.call_count == 1


class TestMainArgParsing:
    @patch("dbbackup.config.load")
    def test_backup_no_job_no_all_errors(self, mock_load):
        """'backup' without job name or --all should error."""
        with pytest.raises(SystemExit):
            with patch("sys.argv", ["dbbackup", "backup"]):
                main()

    @patch("dbbackup.config.load")
    def test_no_command_errors(self, mock_load):
        """No subcommand should error."""
        with pytest.raises(SystemExit):
            with patch("sys.argv", ["dbbackup"]):
                main()

    @patch("dbbackup.config.load")
    def test_backup_all_flag_with_job_uses_all(self, mock_load, tmp_path):
        """'backup job1 --all' → --all takes precedence (job is optional)."""
        cfg_path = _write_config(tmp_path)
        mock_load.return_value = yaml.safe_load(open(cfg_path))

        with patch("sys.argv", ["dbbackup", "backup", "job1", "--all"]):
            with patch("dbbackup.cmd_backup") as mock_cmd:
                main()
                args = mock_cmd.call_args[0][0]
                assert args.all is True
                assert args.job == "job1"
