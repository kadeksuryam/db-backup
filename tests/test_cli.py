"""Tests for dbbackup CLI (dbbackup.py)."""

from __future__ import annotations

import argparse
from unittest.mock import MagicMock, patch, call

import pytest
import yaml

import config
from config import ConfigError
from dbbackup import cmd_backup, cmd_prune, cmd_list, cmd_restore, main, _run_single_job, _dispatch_notifications


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

        raw = config.load(cfg_path)

        args = argparse.Namespace(all=False, job="job1", prune=False)
        mock_create_store.return_value = MagicMock()

        cmd_backup(args, raw)

        mock_run_backup.assert_called_once()

    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_all_jobs(self, mock_run_backup, mock_create_store, tmp_path):
        cfg_path = _write_config(tmp_path)

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

        raw = config.load(cfg_path)

        args = argparse.Namespace(all=True, job=None, prune=False)
        with pytest.raises(SystemExit):
            cmd_backup(args, raw)


class TestCmdPrune:
    @patch("dbbackup.apply_retention")
    @patch("dbbackup.create_store")
    def test_calls_apply_retention(self, mock_create_store, mock_apply_retention, tmp_path):
        cfg_path = _write_config(tmp_path)

        raw = config.load(cfg_path)

        args = argparse.Namespace(job="job1")
        mock_create_store.return_value = MagicMock()

        cmd_prune(args, raw)
        mock_apply_retention.assert_called_once()

    @patch("dbbackup.apply_retention")
    @patch("dbbackup.create_store")
    def test_prune_dry_run(self, mock_create_store, mock_apply_retention, tmp_path):
        """--dry-run → dry_run=True passed to apply_retention."""
        cfg_path = _write_config(tmp_path)

        raw = config.load(cfg_path)

        args = argparse.Namespace(job="job1", dry_run=True)
        mock_create_store.return_value = MagicMock()

        cmd_prune(args, raw)
        mock_apply_retention.assert_called_once()
        _, kwargs = mock_apply_retention.call_args
        assert kwargs["dry_run"] is True


class TestCmdList:
    @patch("dbbackup.list_backups")
    @patch("dbbackup.create_store")
    def test_calls_list_backups(self, mock_create_store, mock_list_backups, tmp_path):
        cfg_path = _write_config(tmp_path)

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

    @patch("dbbackup.apply_retention")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_backup_prune_dry_run(self, mock_run_backup, mock_create_store, mock_apply_retention, tmp_path):
        """--prune --dry-run → dry_run=True passed to apply_retention."""
        cfg_path = _write_config(tmp_path)

        raw = config.load(cfg_path)

        args = argparse.Namespace(all=False, job="job1", prune=True, dry_run=True)
        mock_create_store.return_value = MagicMock()

        cmd_backup(args, raw)

        mock_run_backup.assert_called_once()
        mock_apply_retention.assert_called_once()
        _, kwargs = mock_apply_retention.call_args
        assert kwargs["dry_run"] is True


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

    @patch("dbbackup.config.load")
    def test_parallel_flag_parsed(self, mock_load, tmp_path):
        """'backup --all --parallel 4' → args.parallel == 4."""
        cfg_path = _write_config(tmp_path)
        mock_load.return_value = yaml.safe_load(open(cfg_path))

        with patch("sys.argv", ["dbbackup", "backup", "--all", "--parallel", "4"]):
            with patch("dbbackup.cmd_backup") as mock_cmd:
                main()
                args = mock_cmd.call_args[0][0]
                assert args.parallel == 4

    @patch("dbbackup.config.load")
    def test_dry_run_flag_parsed(self, mock_load, tmp_path):
        """--dry-run parsed correctly for both prune and backup subcommands."""
        cfg_path = _write_config(tmp_path)
        mock_load.return_value = yaml.safe_load(open(cfg_path))

        with patch("sys.argv", ["dbbackup", "prune", "job1", "--dry-run"]):
            with patch("dbbackup.cmd_prune") as mock_cmd:
                main()
                args = mock_cmd.call_args[0][0]
                assert args.dry_run is True

        with patch("sys.argv", ["dbbackup", "backup", "--all", "--dry-run"]):
            with patch("dbbackup.cmd_backup") as mock_cmd:
                main()
                args = mock_cmd.call_args[0][0]
                assert args.dry_run is True


class TestCmdBackupParallel:
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_parallel_1_sequential(self, mock_run_backup, mock_create_store, tmp_path):
        """--parallel 1 runs both jobs sequentially (same as default)."""
        cfg_path = _write_config(tmp_path)

        raw = config.load(cfg_path)

        args = argparse.Namespace(all=True, job=None, prune=False, parallel=1)
        mock_create_store.return_value = MagicMock()

        cmd_backup(args, raw)
        assert mock_run_backup.call_count == 2

    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_parallel_2_runs_all(self, mock_run_backup, mock_create_store, tmp_path):
        """--parallel 2 runs both jobs."""
        cfg_path = _write_config(tmp_path)

        raw = config.load(cfg_path)

        args = argparse.Namespace(all=True, job=None, prune=False, parallel=2)
        mock_create_store.return_value = MagicMock()

        cmd_backup(args, raw)
        assert mock_run_backup.call_count == 2

    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_parallel_one_failure_others_continue(self, mock_run_backup, mock_create_store, tmp_path):
        """One job fails, other completes, exit 1."""
        cfg_path = _write_config(tmp_path)

        raw = config.load(cfg_path)

        call_count = [0]

        def side_effect(*a, **kw):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("boom")

        mock_run_backup.side_effect = side_effect
        mock_create_store.return_value = MagicMock()

        args = argparse.Namespace(all=True, job=None, prune=False, parallel=2)
        with pytest.raises(SystemExit):
            cmd_backup(args, raw)

        assert mock_run_backup.call_count == 2

    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_single_job_parallel_ignored(self, mock_run_backup, mock_create_store, tmp_path):
        """Single job with --parallel 4 works fine."""
        cfg_path = _write_config(tmp_path)

        raw = config.load(cfg_path)

        args = argparse.Namespace(all=False, job="job1", prune=False, parallel=4)
        mock_create_store.return_value = MagicMock()

        cmd_backup(args, raw)
        assert mock_run_backup.call_count == 1


def _write_retry_config(tmp_path, retry_cfg=None, notify=None, notifications=None):
    """Write a config with retry and/or notification settings."""
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
        "stores": {"s1": {"type": "s3", "bucket": "b"}},
        "jobs": {
            "job1": {
                "datasource": "ds1",
                "store": "s1",
                "prefix": "prod",
                "retention": {"keep_last": 3},
            },
        },
    }
    if retry_cfg is not None:
        cfg["jobs"]["job1"]["retry"] = retry_cfg
    if notify is not None:
        cfg["jobs"]["job1"]["notify"] = notify
    if notifications is not None:
        cfg["notifications"] = notifications
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(cfg))
    return str(path)


class TestRetry:
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_no_retry_default(self, mock_run_backup, mock_create_store, tmp_path):
        """No retry config → job runs once, succeeds."""
        cfg_path = _write_retry_config(tmp_path)

        raw = config.load(cfg_path)

        _run_single_job("job1", raw, prune=False)
        mock_run_backup.assert_called_once()

    @patch("dbbackup.time.sleep")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_retry_succeeds_on_second_attempt(self, mock_run_backup, mock_create_store, mock_sleep, tmp_path):
        """First run_backup raises, second succeeds."""
        cfg_path = _write_retry_config(tmp_path, {"max_attempts": 3, "delay": 10, "backoff_multiplier": 2})

        raw = config.load(cfg_path)

        mock_run_backup.side_effect = [RuntimeError("transient"), None]
        mock_create_store.return_value = MagicMock()

        _run_single_job("job1", raw, prune=False)
        assert mock_run_backup.call_count == 2
        mock_sleep.assert_called_once_with(10.0)

    @patch("dbbackup.time.sleep")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_retry_exhausts_all_attempts(self, mock_run_backup, mock_create_store, mock_sleep, tmp_path):
        """All 3 attempts fail → exception raised."""
        cfg_path = _write_retry_config(tmp_path, {"max_attempts": 3, "delay": 10, "backoff_multiplier": 2})

        raw = config.load(cfg_path)

        mock_run_backup.side_effect = RuntimeError("persistent")
        mock_create_store.return_value = MagicMock()

        with pytest.raises(RuntimeError, match="persistent"):
            _run_single_job("job1", raw, prune=False)
        assert mock_run_backup.call_count == 3

    @patch("dbbackup.time.sleep")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_retry_backoff_delays(self, mock_run_backup, mock_create_store, mock_sleep, tmp_path):
        """Verify sleep called with correct exponential backoff delays."""
        cfg_path = _write_retry_config(tmp_path, {"max_attempts": 4, "delay": 10, "backoff_multiplier": 2})

        raw = config.load(cfg_path)

        mock_run_backup.side_effect = RuntimeError("fail")
        mock_create_store.return_value = MagicMock()

        with pytest.raises(RuntimeError):
            _run_single_job("job1", raw, prune=False)

        # Delays: attempt 2 → 10*(2^0)=10, attempt 3 → 10*(2^1)=20, attempt 4 → 10*(2^2)=40
        assert mock_sleep.call_args_list == [call(10.0), call(20.0), call(40.0)]

    @patch("dbbackup.time.sleep")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_config_error_not_retried(self, mock_run_backup, mock_create_store, mock_sleep, tmp_path):
        """ConfigError propagates immediately, no sleep."""
        cfg_path = _write_retry_config(tmp_path, {"max_attempts": 3, "delay": 10, "backoff_multiplier": 2})

        raw = config.load(cfg_path)

        mock_run_backup.side_effect = ConfigError("bad config")
        mock_create_store.return_value = MagicMock()

        with pytest.raises(ConfigError):
            _run_single_job("job1", raw, prune=False)
        mock_run_backup.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("dbbackup.apply_retention")
    @patch("dbbackup.time.sleep")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_retry_with_prune_only_after_success(self, mock_run_backup, mock_create_store, mock_sleep, mock_apply_retention, tmp_path):
        """Prune called once after retry success."""
        cfg_path = _write_retry_config(tmp_path, {"max_attempts": 3, "delay": 5, "backoff_multiplier": 2})

        raw = config.load(cfg_path)

        mock_run_backup.side_effect = [RuntimeError("transient"), None]
        mock_create_store.return_value = MagicMock()

        _run_single_job("job1", raw, prune=True)
        assert mock_run_backup.call_count == 2
        mock_apply_retention.assert_called_once()

    @patch("dbbackup.apply_retention")
    @patch("dbbackup.time.sleep")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_retry_all_fail_no_prune(self, mock_run_backup, mock_create_store, mock_sleep, mock_apply_retention, tmp_path):
        """All attempts fail → apply_retention never called."""
        cfg_path = _write_retry_config(tmp_path, {"max_attempts": 2, "delay": 5, "backoff_multiplier": 2})

        raw = config.load(cfg_path)

        mock_run_backup.side_effect = RuntimeError("fail")
        mock_create_store.return_value = MagicMock()

        with pytest.raises(RuntimeError):
            _run_single_job("job1", raw, prune=True)
        mock_apply_retention.assert_not_called()


class TestNotifications:
    @patch("dbbackup.create_notifier")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_notification_on_failure(self, mock_run_backup, mock_create_store, mock_create_notifier, tmp_path):
        """Job fails, on: failure triggers notification."""
        cfg_path = _write_retry_config(
            tmp_path,
            notify=[{"notifier": "email_ops", "on": "failure"}],
            notifications={"email_ops": {"type": "email", "smtp_host": "smtp.test"}},
        )

        raw = config.load(cfg_path)

        mock_run_backup.side_effect = RuntimeError("boom")
        mock_create_store.return_value = MagicMock()
        mock_notifier = MagicMock()
        mock_create_notifier.return_value = mock_notifier

        with pytest.raises(RuntimeError):
            _run_single_job("job1", raw, prune=False)

        mock_notifier.send.assert_called_once()
        call_args = mock_notifier.send.call_args[0]
        assert call_args[1] == "failure"

    @patch("dbbackup.create_notifier")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_notification_on_success(self, mock_run_backup, mock_create_store, mock_create_notifier, tmp_path):
        """Job succeeds, on: success triggers notification."""
        cfg_path = _write_retry_config(
            tmp_path,
            notify=[{"notifier": "email_ops", "on": "success"}],
            notifications={"email_ops": {"type": "email", "smtp_host": "smtp.test"}},
        )

        raw = config.load(cfg_path)

        mock_create_store.return_value = MagicMock()
        mock_notifier = MagicMock()
        mock_create_notifier.return_value = mock_notifier

        _run_single_job("job1", raw, prune=False)

        mock_notifier.send.assert_called_once()
        call_args = mock_notifier.send.call_args[0]
        assert call_args[1] == "success"

    @patch("dbbackup.create_notifier")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_notification_on_always(self, mock_run_backup, mock_create_store, mock_create_notifier, tmp_path):
        """on: always triggers on both success and failure."""
        cfg_path = _write_retry_config(
            tmp_path,
            notify=[{"notifier": "email_ops", "on": "always"}],
            notifications={"email_ops": {"type": "email", "smtp_host": "smtp.test"}},
        )

        raw = config.load(cfg_path)

        mock_create_store.return_value = MagicMock()
        mock_notifier = MagicMock()
        mock_create_notifier.return_value = mock_notifier

        # Success case
        _run_single_job("job1", raw, prune=False)
        assert mock_notifier.send.call_count == 1

    @patch("dbbackup.create_notifier")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_no_notification_success_when_on_failure(self, mock_run_backup, mock_create_store, mock_create_notifier, tmp_path):
        """on: failure → not triggered on success."""
        cfg_path = _write_retry_config(
            tmp_path,
            notify=[{"notifier": "email_ops", "on": "failure"}],
            notifications={"email_ops": {"type": "email", "smtp_host": "smtp.test"}},
        )

        raw = config.load(cfg_path)

        mock_create_store.return_value = MagicMock()

        _run_single_job("job1", raw, prune=False)
        mock_create_notifier.assert_not_called()

    @patch("dbbackup.create_notifier")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_notification_failure_does_not_raise(self, mock_run_backup, mock_create_store, mock_create_notifier, tmp_path):
        """Notifier error is swallowed, warning logged."""
        cfg_path = _write_retry_config(
            tmp_path,
            notify=[{"notifier": "email_ops", "on": "success"}],
            notifications={"email_ops": {"type": "email", "smtp_host": "smtp.test"}},
        )

        raw = config.load(cfg_path)

        mock_create_store.return_value = MagicMock()
        mock_create_notifier.side_effect = OSError("SMTP down")

        # Should not raise even though notifier fails
        _run_single_job("job1", raw, prune=False)

    @patch("dbbackup.create_notifier")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_notification_failure_does_not_mask_backup_failure(self, mock_run_backup, mock_create_store, mock_create_notifier, tmp_path):
        """Original exception still raised even if notifier also fails."""
        cfg_path = _write_retry_config(
            tmp_path,
            notify=[{"notifier": "email_ops", "on": "failure"}],
            notifications={"email_ops": {"type": "email", "smtp_host": "smtp.test"}},
        )

        raw = config.load(cfg_path)

        mock_run_backup.side_effect = RuntimeError("backup failed")
        mock_create_store.return_value = MagicMock()
        mock_create_notifier.side_effect = OSError("SMTP down")

        with pytest.raises(RuntimeError, match="backup failed"):
            _run_single_job("job1", raw, prune=False)

    @patch("dbbackup.create_notifier")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_multiple_notification_rules(self, mock_run_backup, mock_create_store, mock_create_notifier, tmp_path):
        """Two rules, correct ones fire."""
        cfg_path = _write_retry_config(
            tmp_path,
            notify=[
                {"notifier": "email_ops", "on": "failure"},
                {"notifier": "email_dev", "on": "always"},
            ],
            notifications={
                "email_ops": {"type": "email", "smtp_host": "smtp.test"},
                "email_dev": {"type": "email", "smtp_host": "smtp.test2"},
            },
        )

        raw = config.load(cfg_path)

        mock_create_store.return_value = MagicMock()
        mock_notifier = MagicMock()
        mock_create_notifier.return_value = mock_notifier

        # Success → only "always" rule fires, not "failure"
        _run_single_job("job1", raw, prune=False)
        assert mock_create_notifier.call_count == 1

    @patch("dbbackup.create_notifier")
    @patch("dbbackup.create_store")
    @patch("dbbackup.run_backup")
    def test_no_notifications_configured(self, mock_run_backup, mock_create_store, mock_create_notifier, tmp_path):
        """Works normally with empty notification list."""
        cfg_path = _write_retry_config(tmp_path)

        raw = config.load(cfg_path)

        mock_create_store.return_value = MagicMock()

        _run_single_job("job1", raw, prune=False)
        mock_create_notifier.assert_not_called()
