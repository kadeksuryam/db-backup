"""Tests for config module."""

from __future__ import annotations

import logging
import os
import tempfile

import pytest
import yaml

import config
from config import ConfigError


class TestBuildPrefix:
    def test_both_parts(self):
        assert config.build_prefix("prod", "mydb") == "prod/mydb"

    def test_empty_prefix(self):
        assert config.build_prefix("", "mydb") == "mydb"

    def test_empty_dbname(self):
        assert config.build_prefix("prod", "") == "prod"

    def test_both_empty(self):
        assert config.build_prefix("", "") == ""


class TestResolveEnv:
    def test_resolves_env_key(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "s3cret")
        result = config.resolve_env({"password_env": "MY_SECRET"})
        assert result == {"password": "s3cret"}

    def test_keeps_normal_keys(self):
        result = config.resolve_env({"host": "localhost", "port": 5432})
        assert result == {"host": "localhost", "port": 5432}

    def test_recursive_dicts(self, monkeypatch):
        monkeypatch.setenv("INNER", "value")
        result = config.resolve_env({"outer": {"key_env": "INNER"}})
        assert result == {"outer": {"key": "value"}}

    def test_missing_env_var_exits(self, monkeypatch):
        monkeypatch.delenv("NONEXISTENT", raising=False)
        with pytest.raises(ConfigError):
            config.resolve_env({"key_env": "NONEXISTENT"})


class TestLoad:
    def test_loads_valid_yaml(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(yaml.dump({"datasources": {}, "stores": {}, "jobs": {}}))
        result = config.load(str(cfg_file))
        assert isinstance(result, dict)
        assert "datasources" in result

    def test_missing_file_exits(self):
        with pytest.raises(ConfigError):
            config.load("/nonexistent/path/config.yaml")

    def test_invalid_yaml_exits(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("just a string")
        with pytest.raises(ConfigError):
            config.load(str(cfg_file))

    def test_env_var_config_path(self, monkeypatch, tmp_path):
        cfg_file = tmp_path / "custom.yaml"
        cfg_file.write_text(yaml.dump({"jobs": {}}))
        monkeypatch.setenv("DBBACKUP_CONFIG", str(cfg_file))
        result = config.load()
        assert "jobs" in result


class TestGetDatasource:
    def _make_config(self, ds_overrides=None):
        ds = {
            "engine": "postgres",
            "host": "db.local",
            "port": 5432,
            "user": "admin",
            "password": "secret",
            "database": "mydb",
        }
        if ds_overrides:
            ds.update(ds_overrides)
        return {"datasources": {"testds": ds}}

    def test_valid_datasource(self):
        raw = self._make_config()
        ds = config.get_datasource(raw, "testds")
        assert ds.name == "testds"
        assert ds.engine == "postgres"
        assert ds.host == "db.local"
        assert ds.port == 5432
        assert ds.user == "admin"
        assert ds.password == "secret"
        assert ds.database == "mydb"

    def test_engine_specific_options(self):
        raw = self._make_config({"pg_version": 14})
        ds = config.get_datasource(raw, "testds")
        assert ds.options == {"pg_version": 14}

    def test_missing_engine_exits(self):
        raw = {"datasources": {"bad": {"host": "x", "port": 5432, "database": "db"}}}
        with pytest.raises(ConfigError):
            config.get_datasource(raw, "bad")

    def test_missing_port_exits(self):
        raw = {"datasources": {"bad": {"engine": "postgres", "host": "x", "database": "db"}}}
        with pytest.raises(ConfigError):
            config.get_datasource(raw, "bad")

    def test_missing_datasource_exits(self):
        raw = {"datasources": {}}
        with pytest.raises(ConfigError):
            config.get_datasource(raw, "nonexistent")

    def test_missing_database_exits(self):
        raw = {"datasources": {"bad": {"engine": "postgres", "host": "x", "port": 5432}}}
        with pytest.raises(ConfigError):
            config.get_datasource(raw, "bad")

    def test_host_defaults_to_localhost(self):
        raw = self._make_config()
        del raw["datasources"]["testds"]["host"]
        ds = config.get_datasource(raw, "testds")
        assert ds.host == "localhost"

    def test_user_defaults_to_empty(self):
        raw = self._make_config()
        del raw["datasources"]["testds"]["user"]
        ds = config.get_datasource(raw, "testds")
        assert ds.user == ""

    def test_password_defaults_to_empty(self):
        raw = self._make_config()
        del raw["datasources"]["testds"]["password"]
        ds = config.get_datasource(raw, "testds")
        assert ds.password == ""

    def test_port_as_string(self):
        """YAML might parse port as string if quoted."""
        raw = self._make_config({"port": "3306"})
        ds = config.get_datasource(raw, "testds")
        assert ds.port == 3306

    def test_multiple_engine_options(self):
        raw = self._make_config({"pg_version": 14, "extra_flag": True})
        ds = config.get_datasource(raw, "testds")
        assert ds.options == {"pg_version": 14, "extra_flag": True}

    def test_env_resolution(self, monkeypatch):
        monkeypatch.setenv("DB_PASS", "resolved_pass")
        raw = {"datasources": {"testds": {
            "engine": "postgres",
            "host": "db.local",
            "port": 5432,
            "user": "admin",
            "password_env": "DB_PASS",
            "database": "mydb",
        }}}
        ds = config.get_datasource(raw, "testds")
        assert ds.password == "resolved_pass"


class TestGetStoreConfig:
    def test_valid_store(self):
        raw = {"stores": {"s1": {"type": "s3", "bucket": "b"}}}
        cfg = config.get_store_config(raw, "s1")
        assert cfg == {"type": "s3", "bucket": "b"}

    def test_missing_store_exits(self):
        raw = {"stores": {}}
        with pytest.raises(ConfigError):
            config.get_store_config(raw, "nonexistent")

    def test_env_resolution(self, monkeypatch):
        monkeypatch.setenv("MY_KEY", "resolved")
        raw = {"stores": {"s1": {"type": "s3", "bucket": "b", "access_key_env": "MY_KEY"}}}
        cfg = config.get_store_config(raw, "s1")
        assert cfg["access_key"] == "resolved"
        assert "access_key_env" not in cfg


class TestGetJob:
    def _make_config(self):
        return {
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
                    "retention": {
                        "keep_last": 3,
                        "keep_daily": 7,
                    },
                }
            },
        }

    def test_valid_job(self):
        raw = self._make_config()
        job = config.get_job(raw, "job1")
        assert job.name == "job1"
        assert job.datasource.database == "db1"
        assert job.prefix == "prod"
        assert job.retention.keep_last == 3
        assert job.retention.keep_daily == 7
        assert job.retention.keep_weekly == 0

    def test_missing_job_exits(self):
        raw = self._make_config()
        with pytest.raises(ConfigError):
            config.get_job(raw, "nonexistent")

    def test_default_retention(self):
        raw = self._make_config()
        del raw["jobs"]["job1"]["retention"]
        job = config.get_job(raw, "job1")
        assert job.retention.keep_last == 0

    def test_missing_prefix_defaults_empty(self):
        raw = self._make_config()
        del raw["jobs"]["job1"]["prefix"]
        job = config.get_job(raw, "job1")
        assert job.prefix == ""

    def test_missing_datasource_key_in_job(self):
        """Job config without 'datasource' key → ConfigError."""
        raw = self._make_config()
        del raw["jobs"]["job1"]["datasource"]
        with pytest.raises(ConfigError, match="datasource"):
            config.get_job(raw, "job1")

    def test_missing_store_key_in_job(self):
        """Job config without 'store' key → ConfigError."""
        raw = self._make_config()
        del raw["jobs"]["job1"]["store"]
        with pytest.raises(ConfigError, match="store"):
            config.get_job(raw, "job1")

    def test_job_verify_default_false(self):
        """No verify key → job.verify is False."""
        raw = self._make_config()
        job = config.get_job(raw, "job1")
        assert job.verify is False

    def test_job_verify_true(self):
        """verify: true → job.verify is True."""
        raw = self._make_config()
        raw["jobs"]["job1"]["verify"] = True
        job = config.get_job(raw, "job1")
        assert job.verify is True


class TestGetAllJobNames:
    def test_returns_names(self):
        raw = {"jobs": {"a": {}, "b": {}, "c": {}}}
        assert config.get_all_job_names(raw) == ["a", "b", "c"]

    def test_empty(self):
        assert config.get_all_job_names({}) == []


class TestEdgeCases:
    def test_empty_yaml_file_exits(self, tmp_path):
        """Empty YAML file returns None from safe_load → should exit."""
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("")
        with pytest.raises(ConfigError):
            config.load(str(cfg_file))

    def test_yaml_with_only_comments_exits(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("# just a comment\n")
        with pytest.raises(ConfigError):
            config.load(str(cfg_file))

    def test_yaml_list_not_dict_exits(self, tmp_path):
        """YAML that parses to a list instead of dict → should exit."""
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("- item1\n- item2\n")
        with pytest.raises(ConfigError):
            config.load(str(cfg_file))

    def test_config_path_is_directory_exits(self, tmp_path):
        """Passing a directory path instead of a file → should exit."""
        with pytest.raises(ConfigError):
            config.load(str(tmp_path))

    def test_port_invalid_string_raises(self):
        """Non-numeric port string → int() raises ValueError."""
        raw = {"datasources": {"bad": {
            "engine": "postgres", "host": "x", "port": "abc", "database": "db",
        }}}
        with pytest.raises(ValueError):
            config.get_datasource(raw, "bad")

    def test_port_zero_raises(self):
        """Port 0 → ConfigError."""
        raw = {"datasources": {"bad": {
            "engine": "postgres", "host": "x", "port": 0, "database": "db",
        }}}
        with pytest.raises(ConfigError, match="invalid port"):
            config.get_datasource(raw, "bad")

    def test_port_negative_raises(self):
        """Port -1 → ConfigError."""
        raw = {"datasources": {"bad": {
            "engine": "postgres", "host": "x", "port": -1, "database": "db",
        }}}
        with pytest.raises(ConfigError, match="invalid port"):
            config.get_datasource(raw, "bad")

    def test_port_too_high_raises(self):
        """Port 70000 → ConfigError."""
        raw = {"datasources": {"bad": {
            "engine": "postgres", "host": "x", "port": 70000, "database": "db",
        }}}
        with pytest.raises(ConfigError, match="invalid port"):
            config.get_datasource(raw, "bad")

    def test_resolve_env_non_string_env_key_kept_as_is(self):
        """Non-string value with '_env' suffix key → kept as-is (not resolved)."""
        # key.endswith("_env") only fires for string values
        result = config.resolve_env({"port_env": 123})
        # Since value (123) is not a string, else branch keeps it as {"port_env": 123}
        assert result == {"port_env": 123}

    def test_resolve_env_empty_dict(self):
        assert config.resolve_env({}) == {}

    def test_missing_datasources_section(self):
        """Config with no 'datasources' key → exit."""
        with pytest.raises(ConfigError):
            config.get_datasource({}, "any")

    def test_missing_stores_section(self):
        """Config with no 'stores' key → exit."""
        with pytest.raises(ConfigError):
            config.get_store_config({}, "any")

    def test_missing_jobs_section(self):
        """Config with no 'jobs' key → exit."""
        with pytest.raises(ConfigError):
            config.get_job({}, "any")


class TestRetryPolicy:
    def _make_config(self, retry_cfg=None):
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
                }
            },
        }
        if retry_cfg is not None:
            cfg["jobs"]["job1"]["retry"] = retry_cfg
        return cfg

    def test_default_retry_policy(self):
        raw = self._make_config()
        job = config.get_job(raw, "job1")
        assert job.retry.max_attempts == 1
        assert job.retry.delay == 30.0
        assert job.retry.backoff_multiplier == 2.0

    def test_custom_retry_policy(self):
        raw = self._make_config({"max_attempts": 5, "delay": 10, "backoff_multiplier": 3})
        job = config.get_job(raw, "job1")
        assert job.retry.max_attempts == 5
        assert job.retry.delay == 10.0
        assert job.retry.backoff_multiplier == 3.0

    def test_partial_retry_policy(self):
        raw = self._make_config({"max_attempts": 3})
        job = config.get_job(raw, "job1")
        assert job.retry.max_attempts == 3
        assert job.retry.delay == 30.0
        assert job.retry.backoff_multiplier == 2.0

    def test_invalid_max_attempts_zero(self):
        raw = self._make_config({"max_attempts": 0})
        with pytest.raises(ConfigError):
            config.get_job(raw, "job1")

    def test_invalid_delay_negative(self):
        raw = self._make_config({"delay": -5})
        with pytest.raises(ConfigError):
            config.get_job(raw, "job1")

    def test_invalid_backoff_less_than_one(self):
        raw = self._make_config({"backoff_multiplier": 0.5})
        with pytest.raises(ConfigError):
            config.get_job(raw, "job1")

    def test_invalid_max_attempts_non_numeric(self):
        """Non-numeric max_attempts → ConfigError."""
        raw = self._make_config({"max_attempts": "abc"})
        with pytest.raises(ConfigError, match="invalid retry config"):
            config.get_job(raw, "job1")

    def test_invalid_delay_non_numeric(self):
        """Non-numeric delay → ConfigError."""
        raw = self._make_config({"delay": "slow"})
        with pytest.raises(ConfigError, match="invalid retry config"):
            config.get_job(raw, "job1")


class TestNotificationConfig:
    def _make_config(self, notify=None, notifications=None):
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
                }
            },
        }
        if notifications is not None:
            cfg["notifications"] = notifications
        if notify is not None:
            cfg["jobs"]["job1"]["notify"] = notify
        return cfg

    def test_job_with_notify_rules(self):
        raw = self._make_config(
            notify=[{"notifier": "email_ops", "on": "failure"}],
            notifications={"email_ops": {"type": "email", "smtp_host": "smtp.test"}},
        )
        job = config.get_job(raw, "job1")
        assert len(job.notifications) == 1
        assert job.notifications[0].notifier_name == "email_ops"
        assert job.notifications[0].on == "failure"

    def test_job_without_notify(self):
        raw = self._make_config()
        job = config.get_job(raw, "job1")
        assert job.notifications == []

    def test_invalid_trigger_exits(self):
        raw = self._make_config(
            notify=[{"notifier": "email_ops", "on": "never"}],
            notifications={"email_ops": {"type": "email", "smtp_host": "smtp.test"}},
        )
        with pytest.raises(ConfigError):
            config.get_job(raw, "job1")

    def test_missing_notifier_key_exits(self):
        raw = self._make_config(
            notify=[{"on": "failure"}],
            notifications={"email_ops": {"type": "email", "smtp_host": "smtp.test"}},
        )
        with pytest.raises(ConfigError):
            config.get_job(raw, "job1")

    def test_notifier_references_nonexistent(self):
        raw = self._make_config(
            notify=[{"notifier": "nonexistent", "on": "failure"}],
            notifications={"email_ops": {"type": "email", "smtp_host": "smtp.test"}},
        )
        with pytest.raises(ConfigError):
            config.get_job(raw, "job1")

    def test_get_notifier_config_valid(self):
        raw = {"notifications": {"email_ops": {"type": "email", "smtp_host": "smtp.test"}}}
        cfg = config.get_notifier_config(raw, "email_ops")
        assert cfg == {"type": "email", "smtp_host": "smtp.test"}

    def test_get_notifier_config_missing(self):
        raw = {"notifications": {}}
        with pytest.raises(ConfigError):
            config.get_notifier_config(raw, "nonexistent")

    def test_get_notifier_config_env_resolution(self, monkeypatch):
        monkeypatch.setenv("SMTP_PASS", "secret123")
        raw = {"notifications": {"n1": {"type": "email", "password_env": "SMTP_PASS"}}}
        cfg = config.get_notifier_config(raw, "n1")
        assert cfg["password"] == "secret123"
        assert "password_env" not in cfg


class TestConfigFilePermissions:
    """Security: warn if config file is readable by group/others."""

    def test_world_readable_warns(self, tmp_path, caplog):

        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(yaml.dump({"jobs": {}}))
        cfg_file.chmod(0o644)  # world-readable
        with caplog.at_level(logging.WARNING):
            config.load(str(cfg_file))
        assert "readable by group/others" in caplog.text

    def test_owner_only_no_warning(self, tmp_path, caplog):

        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(yaml.dump({"jobs": {}}))
        cfg_file.chmod(0o600)  # owner-only
        with caplog.at_level(logging.WARNING):
            config.load(str(cfg_file))
        assert "readable by group/others" not in caplog.text

    def test_group_readable_warns(self, tmp_path, caplog):

        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(yaml.dump({"jobs": {}}))
        cfg_file.chmod(0o640)  # group-readable
        with caplog.at_level(logging.WARNING):
            config.load(str(cfg_file))
        assert "readable by group/others" in caplog.text
