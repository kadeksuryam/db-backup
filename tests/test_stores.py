"""Tests for stores package — factory, S3Store, SSHStore."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from config import ConfigError
from stores import create_store, BackupInfo, BACKUP_EXTENSIONS, Store
from stores.s3 import S3Store
from stores.ssh import SSHStore


class TestCreateStore:
    @patch("stores.s3.boto3")
    def test_creates_s3(self, mock_boto):
        store = create_store({"type": "s3", "bucket": "b"})
        assert isinstance(store, S3Store)

    def test_creates_ssh(self):
        store = create_store({
            "type": "ssh", "host": "h", "user": "u", "path": "/data",
        })
        assert isinstance(store, SSHStore)

    def test_unknown_type_raises(self):
        with pytest.raises(ConfigError, match="Unknown store type"):
            create_store({"type": "gcs"})


class TestS3Store:
    @patch("stores.s3.os.path.getsize", return_value=1024)
    @patch("stores.s3.boto3")
    def test_upload(self, mock_boto, mock_getsize):
        mock_client = MagicMock()
        mock_boto.session.Session.return_value.client.return_value = mock_client
        mock_client.head_object.return_value = {"ContentLength": 1024}

        store = S3Store(bucket="mybucket")
        store.upload("/tmp/file.sql.gz", "prefix/file.sql.gz")
        mock_client.upload_file.assert_called_once_with(
            "/tmp/file.sql.gz", "mybucket", "prefix/file.sql.gz"
        )
        mock_client.head_object.assert_called_once_with(
            Bucket="mybucket", Key="prefix/file.sql.gz"
        )

    @patch("stores.s3.os.path.getsize", return_value=1024)
    @patch("stores.s3.boto3")
    def test_upload_size_mismatch_raises(self, mock_boto, mock_getsize):
        """Upload succeeds but remote size differs → RuntimeError."""
        mock_client = MagicMock()
        mock_boto.session.Session.return_value.client.return_value = mock_client
        mock_client.head_object.return_value = {"ContentLength": 512}

        store = S3Store(bucket="mybucket")
        with pytest.raises(RuntimeError, match="Upload verification failed"):
            store.upload("/tmp/file.sql.gz", "prefix/file.sql.gz")

    @patch("stores.s3.boto3")
    def test_download(self, mock_boto):
        mock_client = MagicMock()
        mock_boto.session.Session.return_value.client.return_value = mock_client

        store = S3Store(bucket="mybucket")
        store.download("prefix/file.sql.gz", "/tmp/file.sql.gz")
        mock_client.download_file.assert_called_once_with(
            "mybucket", "prefix/file.sql.gz", "/tmp/file.sql.gz"
        )

    @patch("stores.s3.boto3")
    def test_list(self, mock_boto):
        mock_client = MagicMock()
        mock_boto.session.Session.return_value.client.return_value = mock_client

        paginator = MagicMock()
        mock_client.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "prod/db/db-20260101-120000.sql.gz", "Size": 1024},
                {"Key": "prod/db/db-20260102-120000.sql.gz", "Size": 2048},
                {"Key": "prod/db/readme.txt", "Size": 100},  # should be skipped
            ]},
        ]

        store = S3Store(bucket="mybucket")
        backups = store.list("prod/db")

        assert len(backups) == 2
        assert backups[0].timestamp < backups[1].timestamp  # sorted oldest-first
        assert backups[0].size == 1024
        assert backups[1].size == 2048

    @patch("stores.s3.boto3")
    def test_list_skips_unparseable(self, mock_boto):
        mock_client = MagicMock()
        mock_boto.session.Session.return_value.client.return_value = mock_client

        paginator = MagicMock()
        mock_client.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "prod/db/not-a-backup.sql.gz", "Size": 100},
            ]},
        ]

        store = S3Store(bucket="mybucket")
        assert store.list("prod/db") == []

    @patch("stores.s3.boto3")
    def test_list_multi_page(self, mock_boto):
        """Paginator returns multiple pages."""
        mock_client = MagicMock()
        mock_boto.session.Session.return_value.client.return_value = mock_client

        paginator = MagicMock()
        mock_client.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "prod/db/db-20260101-120000.sql.gz", "Size": 1024},
            ]},
            {"Contents": [
                {"Key": "prod/db/db-20260102-120000.sql.gz", "Size": 2048},
            ]},
        ]

        store = S3Store(bucket="mybucket")
        backups = store.list("prod/db")
        assert len(backups) == 2
        assert backups[0].timestamp < backups[1].timestamp

    @patch("stores.s3.boto3")
    def test_list_empty_page(self, mock_boto):
        """Page without 'Contents' key (empty prefix)."""
        mock_client = MagicMock()
        mock_boto.session.Session.return_value.client.return_value = mock_client

        paginator = MagicMock()
        mock_client.get_paginator.return_value = paginator
        paginator.paginate.return_value = [{}]  # no Contents

        store = S3Store(bucket="mybucket")
        assert store.list("prod/db") == []

    @patch("stores.s3.boto3")
    def test_list_file_at_root(self, mock_boto):
        """File key with no '/' — filename is the key itself."""
        mock_client = MagicMock()
        mock_boto.session.Session.return_value.client.return_value = mock_client

        paginator = MagicMock()
        mock_client.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "db-20260101-120000.sql.gz", "Size": 500},
            ]},
        ]

        store = S3Store(bucket="mybucket")
        backups = store.list("")
        assert len(backups) == 1
        assert backups[0].filename == "db-20260101-120000.sql.gz"

    @patch("stores.s3.boto3")
    def test_delete(self, mock_boto):
        mock_client = MagicMock()
        mock_boto.session.Session.return_value.client.return_value = mock_client

        store = S3Store(bucket="mybucket")
        store.delete("prefix/file.sql.gz")
        mock_client.delete_object.assert_called_once_with(
            Bucket="mybucket", Key="prefix/file.sql.gz"
        )

    @patch("stores.s3.boto3")
    def test_list_mixed_extensions(self, mock_boto):
        """S3 list recognizes all supported backup extensions."""
        mock_client = MagicMock()
        mock_boto.session.Session.return_value.client.return_value = mock_client

        paginator = MagicMock()
        mock_client.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "prod/db/db-20260101-120000.sql.gz", "Size": 1024},
                {"Key": "prod/db/db-20260102-120000.sql.zst", "Size": 2048},
                {"Key": "prod/db/db-20260103-120000.dump.lz4", "Size": 512},
                {"Key": "prod/db/db-20260104-120000.dump", "Size": 4096},
                {"Key": "prod/db/db-20260105-120000.sql", "Size": 8192},
            ]},
        ]

        store = S3Store(bucket="mybucket")
        backups = store.list("prod/db")
        assert len(backups) == 5
        assert backups[0].timestamp < backups[-1].timestamp

    @patch("stores.s3.boto3")
    def test_list_skips_unknown_extensions(self, mock_boto):
        """S3 list skips files with unrecognized extensions like .tar.gz."""
        mock_client = MagicMock()
        mock_boto.session.Session.return_value.client.return_value = mock_client

        paginator = MagicMock()
        mock_client.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "prod/db/db-20260101-120000.tar.gz", "Size": 1024},
                {"Key": "prod/db/db-20260102-120000.sql.gz", "Size": 2048},
            ]},
        ]

        store = S3Store(bucket="mybucket")
        backups = store.list("prod/db")
        assert len(backups) == 1
        assert backups[0].filename == "db-20260102-120000.sql.gz"


class TestSSHStore:
    def _store(self):
        return SSHStore(host="backup.host", user="backupuser", path="/data/backups", port=2222)

    @patch("stores.ssh.subprocess.run")
    def test_upload(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        store = self._store()
        store.upload("/tmp/file.sql.gz", "prod/db/file.sql.gz")

        # Two calls: mkdir -p, then scp
        assert mock_run.call_count == 2
        mkdir_cmd = mock_run.call_args_list[0][0][0]
        scp_cmd = mock_run.call_args_list[1][0][0]

        assert "ssh" in mkdir_cmd[0]
        assert "mkdir" in " ".join(mkdir_cmd)
        assert "scp" in scp_cmd[0]
        assert "-P" in scp_cmd  # uppercase P for scp port

    @patch("stores.ssh.subprocess.run")
    def test_download(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        store = self._store()
        store.download("prod/db/file.sql.gz", "/tmp/file.sql.gz")

        assert mock_run.call_count == 1
        cmd = mock_run.call_args[0][0]
        assert "scp" in cmd[0]

    @patch("stores.ssh.subprocess.run")
    def test_list_parses_output(self, mock_run):
        output = (
            "1024\t/data/backups/prod/db/db-20260101-120000.sql.gz\n"
            "2048\t/data/backups/prod/db/db-20260102-120000.sql.gz\n"
        )
        mock_run.return_value = MagicMock(returncode=0, stdout=output)

        store = self._store()
        backups = store.list("prod/db")

        assert len(backups) == 2
        assert backups[0].key == "prod/db/db-20260101-120000.sql.gz"
        assert backups[0].size == 1024
        assert backups[1].key == "prod/db/db-20260102-120000.sql.gz"
        assert backups[1].size == 2048
        assert backups[0].timestamp < backups[1].timestamp

    @patch("stores.ssh.subprocess.run")
    def test_list_empty(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        store = self._store()
        assert store.list("prod/db") == []

    @patch("stores.ssh.subprocess.run")
    def test_list_malformed_lines_skipped(self, mock_run):
        """Lines without a tab separator are silently skipped."""
        output = (
            "1024\t/data/backups/prod/db/db-20260101-120000.sql.gz\n"
            "this line has no tab\n"
            "2048\t/data/backups/prod/db/db-20260102-120000.sql.gz\n"
        )
        mock_run.return_value = MagicMock(returncode=0, stdout=output)
        store = self._store()
        backups = store.list("prod/db")
        assert len(backups) == 2

    @patch("stores.ssh.subprocess.run")
    def test_list_unparseable_timestamp_skipped(self, mock_run):
        """Files with unparseable timestamps are silently skipped."""
        output = (
            "1024\t/data/backups/prod/db/db-20260101-120000.sql.gz\n"
            "500\t/data/backups/prod/db/random-file.sql.gz\n"
        )
        mock_run.return_value = MagicMock(returncode=0, stdout=output)
        store = self._store()
        backups = store.list("prod/db")
        assert len(backups) == 1
        assert backups[0].filename == "db-20260101-120000.sql.gz"

    @patch("stores.ssh.subprocess.run")
    def test_list_blank_lines_skipped(self, mock_run):
        """Blank lines in output are skipped."""
        output = "\n\n1024\t/data/backups/prod/db/db-20260101-120000.sql.gz\n\n"
        mock_run.return_value = MagicMock(returncode=0, stdout=output)
        store = self._store()
        backups = store.list("prod/db")
        assert len(backups) == 1

    @patch("stores.ssh.subprocess.run")
    def test_upload_constructs_correct_remote_path(self, mock_run):
        """Verify the remote path is base_path + remote_key."""
        mock_run.return_value = MagicMock(returncode=0)
        store = self._store()
        store.upload("/tmp/file.sql.gz", "prod/db/file.sql.gz")

        scp_cmd = mock_run.call_args_list[1][0][0]
        scp_cmd_str = " ".join(scp_cmd)
        assert "backupuser@backup.host:/data/backups/prod/db/file.sql.gz" in scp_cmd_str

    @patch("stores.ssh.subprocess.run")
    def test_delete(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        store = self._store()
        store.delete("prod/db/file.sql.gz")

        cmd = mock_run.call_args[0][0]
        assert "ssh" in cmd[0]
        assert "rm -f" in " ".join(cmd)

    @patch("stores.ssh.subprocess.run")
    def test_command_failure_raises(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stderr="Permission denied")
        store = self._store()
        with pytest.raises(RuntimeError, match="Command failed"):
            store.upload("/tmp/f", "k")

    def test_connect_opts_ssh_vs_scp(self):
        """SSH uses -p (lowercase), SCP uses -P (uppercase) for port."""
        store = SSHStore(host="h", user="u", path="/p", port=2222, key_file="/key")
        ssh = store._ssh_opts()
        scp = store._scp_opts()

        assert "-p" in ssh and "2222" in ssh
        assert "-P" in scp and "2222" in scp
        # Both have key_file
        assert "-i" in ssh and "/key" in ssh
        assert "-i" in scp and "/key" in scp

    def test_connect_opts_no_keyfile(self):
        store = SSHStore(host="h", user="u", path="/p")
        opts = store._ssh_opts()
        assert "-i" not in opts

    @patch("stores.ssh.subprocess.run")
    def test_list_mixed_extensions(self, mock_run):
        """SSH list parses mixed backup extensions."""
        output = (
            "1024\t/data/backups/prod/db/db-20260101-120000.sql.gz\n"
            "2048\t/data/backups/prod/db/db-20260102-120000.dump.zst\n"
            "512\t/data/backups/prod/db/db-20260103-120000.sql.lz4\n"
        )
        mock_run.return_value = MagicMock(returncode=0, stdout=output)
        store = self._store()
        backups = store.list("prod/db")
        assert len(backups) == 3

    @patch("stores.ssh.subprocess.run")
    def test_list_find_includes_all_extensions(self, mock_run):
        """SSH find command includes patterns for all backup extensions."""
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        store = self._store()
        store.list("prod/db")

        cmd = mock_run.call_args[0][0]
        ssh_cmd_str = cmd[-1]
        for ext in BACKUP_EXTENSIONS:
            assert f"'*{ext}'" in ssh_cmd_str
        # Verify -printf is used instead of shell pipeline
        assert "-printf" in ssh_cmd_str


class TestCreateStoreEdgeCases:
    def test_missing_type_key_raises(self):
        """Config with no 'type' key → raises ConfigError."""
        with pytest.raises(ConfigError, match="Unknown store type 'None'"):
            create_store({"bucket": "b"})

    def test_empty_config_raises(self):
        with pytest.raises(ConfigError):
            create_store({})

    @patch("stores.s3.boto3")
    def test_s3_missing_bucket_raises_config_error(self, mock_boto):
        """S3 create() without 'bucket' → ConfigError."""
        from stores.s3 import create
        with pytest.raises(ConfigError, match="bucket"):
            create({"endpoint": "https://r2.example.com"})

    def test_ssh_missing_host_raises_config_error(self):
        """SSH create() without 'host' → ConfigError."""
        from stores.ssh import create
        with pytest.raises(ConfigError, match="host"):
            create({"user": "u", "path": "/p"})

    def test_ssh_missing_user_raises_config_error(self):
        """SSH create() without 'user' → ConfigError."""
        from stores.ssh import create
        with pytest.raises(ConfigError, match="user"):
            create({"host": "h", "path": "/p"})

    def test_ssh_missing_path_raises_config_error(self):
        """SSH create() without 'path' → ConfigError."""
        from stores.ssh import create
        with pytest.raises(ConfigError, match="path"):
            create({"host": "h", "user": "u"})


class TestS3StoreEdgeCases:
    @patch("stores.s3.boto3")
    def test_custom_endpoint(self, mock_boto):
        """S3Store with custom endpoint (e.g. R2/MinIO)."""
        store = S3Store(bucket="mybucket", endpoint="https://r2.example.com")
        # Should construct with endpoint_url
        session_call = mock_boto.session.Session.return_value.client
        assert session_call.called
        kwargs = session_call.call_args[1]
        assert kwargs["endpoint_url"] == "https://r2.example.com"

    @patch("stores.s3.boto3")
    def test_no_endpoint_omits_url(self, mock_boto):
        """S3Store without endpoint → no endpoint_url in client kwargs."""
        store = S3Store(bucket="mybucket")
        kwargs = mock_boto.session.Session.return_value.client.call_args[1]
        assert "endpoint_url" not in kwargs

    @patch("stores.s3.boto3")
    def test_list_object_without_size(self, mock_boto):
        """S3 object metadata missing 'Size' → defaults to 0."""
        mock_client = MagicMock()
        mock_boto.session.Session.return_value.client.return_value = mock_client

        paginator = MagicMock()
        mock_client.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "prod/db/db-20260101-120000.sql.gz"},  # no Size
            ]},
        ]

        store = S3Store(bucket="mybucket")
        backups = store.list("prod/db")
        assert len(backups) == 1
        assert backups[0].size == 0


class TestSSHStoreEdgeCases:
    def test_default_port(self):
        """SSHStore without port → defaults to 22."""
        store = SSHStore(host="h", user="u", path="/p")
        assert store._port == 22
        opts = store._ssh_opts()
        assert "22" in opts

    @patch("stores.ssh.subprocess.run")
    def test_command_failure_empty_stderr(self, mock_run):
        """Command fails with empty stderr → still raises."""
        mock_run.return_value = MagicMock(returncode=1, stderr="")
        store = SSHStore(host="h", user="u", path="/p")
        with pytest.raises(RuntimeError, match="Command failed"):
            store.upload("/tmp/f", "k")

    @patch("stores.ssh.subprocess.run")
    def test_list_non_numeric_size(self, mock_run):
        """Non-numeric size from find -printf → should raise ValueError."""
        output = "NaN\t/data/backups/prod/db/db-20260101-120000.sql.gz\n"
        mock_run.return_value = MagicMock(returncode=0, stdout=output)
        store = SSHStore(host="h", user="u", path="/data/backups")
        with pytest.raises(ValueError):
            store.list("prod/db")

    def test_ssh_factory_creates_store(self):
        """stores.ssh.create() with full config."""
        from stores.ssh import create
        store = create({
            "host": "backup.host",
            "user": "backupuser",
            "path": "/data",
            "port": 2222,
            "key_file": "/home/user/.ssh/id_rsa",
        })
        assert isinstance(store, SSHStore)
        assert store._port == 2222
        assert store._key_file == "/home/user/.ssh/id_rsa"

    def test_ssh_factory_defaults(self):
        """stores.ssh.create() uses defaults for optional fields."""
        from stores.ssh import create
        store = create({"host": "h", "user": "u", "path": "/p"})
        assert store._port == 22
        assert store._key_file is None


class TestSSHShellInjectionPrevention:
    """Security: paths interpolated into SSH commands must be shell-escaped."""

    @patch("stores.ssh.subprocess.run")
    def test_upload_mkdir_escapes_path(self, mock_run):
        """mkdir -p command should escape the remote dir."""
        mock_run.return_value = MagicMock(returncode=0)
        store = SSHStore(host="h", user="u", path="/data")
        store.upload("/tmp/f.sql.gz", "prefix/db/file.sql.gz")

        mkdir_cmd = mock_run.call_args_list[0][0][0]
        # The ssh command string (last arg) should contain a quoted path
        ssh_cmd_str = mkdir_cmd[-1]
        # shlex.quote wraps in single quotes for simple paths
        assert "'/data/prefix/db'" in ssh_cmd_str or "/data/prefix/db" in ssh_cmd_str

    @patch("stores.ssh.subprocess.run")
    def test_upload_escapes_malicious_path(self, mock_run):
        """Malicious remote_key with shell metacharacters should be escaped."""
        mock_run.return_value = MagicMock(returncode=0)
        store = SSHStore(host="h", user="u", path="/data")
        store.upload("/tmp/f", "$(whoami)/file.sql.gz")

        mkdir_cmd = mock_run.call_args_list[0][0][0]
        ssh_cmd_str = mkdir_cmd[-1]
        # The $(whoami) should be quoted, not executed
        assert "$(whoami)" not in ssh_cmd_str or "'" in ssh_cmd_str

    @patch("stores.ssh.subprocess.run")
    def test_delete_escapes_path(self, mock_run):
        """rm -f command should escape the remote path."""
        mock_run.return_value = MagicMock(returncode=0)
        store = SSHStore(host="h", user="u", path="/data")
        store.delete("foo; rm -rf /")

        cmd = mock_run.call_args[0][0]
        ssh_cmd_str = cmd[-1]
        # The malicious path should be quoted
        assert "rm -f '/data/foo; rm -rf /'" in ssh_cmd_str

    @patch("stores.ssh.subprocess.run")
    def test_list_escapes_path(self, mock_run):
        """find command should escape the remote dir."""
        mock_run.return_value = MagicMock(returncode=0, stdout="")
        store = SSHStore(host="h", user="u", path="/data")
        store.list("$(whoami)")

        cmd = mock_run.call_args[0][0]
        ssh_cmd_str = cmd[-1]
        # Should NOT contain unquoted $(whoami)
        assert "'$(whoami)'" in ssh_cmd_str or "'/data/$(whoami)'" in ssh_cmd_str

    def test_strict_host_key_checking_accept_new(self):
        """SSH should use accept-new, not 'no'."""
        store = SSHStore(host="h", user="u", path="/p")
        opts = store._ssh_opts()
        assert "StrictHostKeyChecking=accept-new" in opts
        assert "StrictHostKeyChecking=no" not in opts


class TestSSHControlMaster:
    def test_connect_opts_has_control_master(self):
        """SSH options include ControlPath, ControlMaster, ControlPersist."""
        store = SSHStore(host="h", user="u", path="/p")
        opts = store._ssh_opts()
        opts_str = " ".join(opts)
        assert "ControlMaster=auto" in opts_str
        assert "ControlPersist=60" in opts_str
        assert "ControlPath=" in opts_str
        store.close()

    @patch("stores.ssh.subprocess.run")
    def test_close_sends_control_exit(self, mock_run):
        """close() sends ssh -O exit to tear down master."""
        store = SSHStore(host="h", user="u", path="/p")
        control_dir = store._control_dir
        assert os.path.isdir(control_dir)

        mock_run.return_value = MagicMock(returncode=0)
        store.close()

        # ssh -O exit should have been called
        assert mock_run.called
        cmd = mock_run.call_args[0][0]
        assert "-O" in cmd
        assert "exit" in cmd

    @patch("stores.ssh.subprocess.run")
    def test_context_manager(self, mock_run):
        """SSHStore works as a context manager and calls close on exit."""
        mock_run.return_value = MagicMock(returncode=0)
        with SSHStore(host="h", user="u", path="/p") as store:
            assert isinstance(store, SSHStore)
            control_dir = store._control_dir
            assert os.path.isdir(control_dir)
        # After exit, close should have been called
        assert mock_run.called


class TestStoreBaseClass:
    """Store ABC has close() and context manager protocol."""

    def test_store_has_close_method(self):
        """Store base class defines a close() method."""
        assert hasattr(Store, "close")

    def test_store_context_manager_protocol(self):
        """Store base class supports context manager protocol."""

        class DummyStore(Store):
            def upload(self, local_path, remote_key): pass
            def download(self, remote_key, local_path): pass
            def list(self, prefix): return []
            def delete(self, remote_key): pass

        closed = []
        store = DummyStore()
        store.close = lambda: closed.append(True)

        with store as s:
            assert s is store

        assert closed == [True]
