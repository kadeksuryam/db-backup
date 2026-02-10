"""Tests for retention module — GFS retention algorithm."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from retention import compute_keep_set, apply_retention
from stores import BackupInfo
from config import RetentionPolicy


def _bi(key: str, days_ago: int, ref: datetime | None = None) -> BackupInfo:
    """Helper: create a BackupInfo with a timestamp N days before ref."""
    ref = ref or datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
    ts = ref - timedelta(days=days_ago)
    return BackupInfo(
        key=key,
        filename=f"db-{ts.strftime('%Y%m%d-%H%M%S')}.sql.gz",
        timestamp=ts,
        size=1000,
    )


class TestComputeKeepSet:
    def test_empty_backups(self):
        policy = RetentionPolicy(keep_last=5)
        assert compute_keep_set([], policy) == set()

    def test_keep_last(self):
        backups = [_bi(f"b{i}", i) for i in range(10)]  # b0=newest ... b9=oldest
        policy = RetentionPolicy(keep_last=3)
        keep = compute_keep_set(backups, policy)
        assert keep == {"b0", "b1", "b2"}

    def test_keep_last_more_than_available(self):
        backups = [_bi("b0", 0), _bi("b1", 1)]
        policy = RetentionPolicy(keep_last=10)
        keep = compute_keep_set(backups, policy)
        assert keep == {"b0", "b1"}

    def test_keep_daily(self):
        # 10 backups, one per day
        backups = [_bi(f"d{i}", i) for i in range(10)]
        policy = RetentionPolicy(keep_daily=3)
        keep = compute_keep_set(backups, policy)
        # Should keep the newest backup from each of the 3 most recent days
        assert keep == {"d0", "d1", "d2"}

    def test_keep_daily_multiple_per_day(self):
        """When multiple backups exist on the same day, keep only the newest."""
        ref = datetime(2026, 2, 10, 18, 0, 0, tzinfo=timezone.utc)
        backups = [
            BackupInfo(key="morning", filename="db-20260210-060000.sql.gz",
                       timestamp=datetime(2026, 2, 10, 6, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="evening", filename="db-20260210-180000.sql.gz",
                       timestamp=datetime(2026, 2, 10, 18, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="yesterday", filename="db-20260209-120000.sql.gz",
                       timestamp=datetime(2026, 2, 9, 12, 0, 0, tzinfo=timezone.utc), size=100),
        ]
        policy = RetentionPolicy(keep_daily=2)
        keep = compute_keep_set(backups, policy, now=ref)
        # Day 2026-02-10: keep "evening" (newest), Day 2026-02-09: keep "yesterday"
        assert keep == {"evening", "yesterday"}

    def test_keep_weekly(self):
        # Backups spanning several weeks
        backups = [_bi(f"w{i}", i * 7) for i in range(5)]
        policy = RetentionPolicy(keep_weekly=2)
        keep = compute_keep_set(backups, policy)
        assert len(keep) == 2
        assert "w0" in keep  # most recent week

    def test_keep_monthly(self):
        ref = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        backups = [
            BackupInfo(key="jun", filename="db-20260615-120000.sql.gz",
                       timestamp=datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="may", filename="db-20260515-120000.sql.gz",
                       timestamp=datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="apr", filename="db-20260415-120000.sql.gz",
                       timestamp=datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="mar", filename="db-20260315-120000.sql.gz",
                       timestamp=datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc), size=100),
        ]
        policy = RetentionPolicy(keep_monthly=3)
        keep = compute_keep_set(backups, policy, now=ref)
        assert keep == {"jun", "may", "apr"}

    def test_keep_yearly(self):
        backups = [
            BackupInfo(key="y2026", filename="db-20260101-120000.sql.gz",
                       timestamp=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="y2025", filename="db-20250101-120000.sql.gz",
                       timestamp=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="y2024", filename="db-20240101-120000.sql.gz",
                       timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc), size=100),
        ]
        policy = RetentionPolicy(keep_yearly=2)
        keep = compute_keep_set(backups, policy)
        assert keep == {"y2026", "y2025"}

    def test_combined_rules_union(self):
        """Multiple rules: a backup kept by ANY rule is protected."""
        ref = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        backups = [_bi(f"b{i}", i, ref) for i in range(30)]
        policy = RetentionPolicy(keep_last=2, keep_daily=5)
        keep = compute_keep_set(backups, policy, now=ref)
        # keep_last protects b0, b1. keep_daily protects b0..b4.
        # Union: b0..b4
        assert keep == {"b0", "b1", "b2", "b3", "b4"}

    def test_no_rules_returns_empty(self):
        backups = [_bi("b0", 0)]
        policy = RetentionPolicy()  # all zeros
        keep = compute_keep_set(backups, policy)
        assert keep == set()

    def test_single_backup_keep_last_1(self):
        backups = [_bi("only", 0)]
        policy = RetentionPolicy(keep_last=1)
        assert compute_keep_set(backups, policy) == {"only"}

    def test_single_backup_keep_daily_1(self):
        backups = [_bi("only", 0)]
        policy = RetentionPolicy(keep_daily=1)
        assert compute_keep_set(backups, policy) == {"only"}

    def test_all_same_day_keep_daily_1(self):
        """Multiple backups on same day, keep_daily=1 → keep newest only."""
        ref = datetime(2026, 2, 10, 23, 0, 0, tzinfo=timezone.utc)
        backups = [
            BackupInfo(key="a", filename="db-20260210-060000.sql.gz",
                       timestamp=datetime(2026, 2, 10, 6, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="b", filename="db-20260210-120000.sql.gz",
                       timestamp=datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="c", filename="db-20260210-180000.sql.gz",
                       timestamp=datetime(2026, 2, 10, 18, 0, 0, tzinfo=timezone.utc), size=100),
        ]
        policy = RetentionPolicy(keep_daily=1)
        keep = compute_keep_set(backups, policy, now=ref)
        assert keep == {"c"}  # newest on that day

    def test_keep_weekly_iso_week_boundary(self):
        """Backups around year boundary — ISO week handles it correctly."""
        backups = [
            BackupInfo(key="dec31", filename="db-20251231-120000.sql.gz",
                       timestamp=datetime(2025, 12, 31, 12, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="jan01", filename="db-20260101-120000.sql.gz",
                       timestamp=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="jan07", filename="db-20260107-120000.sql.gz",
                       timestamp=datetime(2026, 1, 7, 12, 0, 0, tzinfo=timezone.utc), size=100),
        ]
        policy = RetentionPolicy(keep_weekly=2)
        keep = compute_keep_set(backups, policy)
        assert len(keep) == 2

    def test_overlapping_rules_no_double_count(self):
        """A backup kept by both keep_last and keep_daily is counted once."""
        ref = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        backups = [_bi(f"b{i}", i, ref) for i in range(5)]
        policy = RetentionPolicy(keep_last=1, keep_daily=1)
        keep = compute_keep_set(backups, policy, now=ref)
        # Both rules keep b0 — only 1 entry in the set
        assert keep == {"b0"}


class TestApplyRetention:
    def test_no_backups(self):
        """No backups → no-op, no errors."""
        class FakeStore:
            def list(self, prefix): return []
            def delete(self, key): pytest.fail("delete should not be called")
        apply_retention(FakeStore(), "pfx", "db", RetentionPolicy(keep_last=1))

    def test_no_rules_keeps_all(self):
        """No retention rules → keeps everything."""
        class FakeStore:
            def list(self, prefix):
                return [_bi("b0", 0), _bi("b1", 1)]
            def delete(self, key): pytest.fail("delete should not be called")
        apply_retention(FakeStore(), "pfx", "db", RetentionPolicy())

    def test_deletes_expired(self):
        deleted = []

        class FakeStore:
            def list(self, prefix):
                return [_bi(f"b{i}", i) for i in range(5)]
            def delete(self, key):
                deleted.append(key)

        apply_retention(FakeStore(), "pfx", "db", RetentionPolicy(keep_last=2))
        # b0, b1 kept; b2, b3, b4 deleted
        assert set(deleted) == {"b2", "b3", "b4"}

    def test_all_kept_nothing_deleted(self):
        """When policy keeps everything, delete is never called."""
        class FakeStore:
            def list(self, prefix):
                return [_bi("b0", 0), _bi("b1", 1)]
            def delete(self, key):
                pytest.fail("delete should not be called")

        apply_retention(FakeStore(), "pfx", "db", RetentionPolicy(keep_last=10))

    def test_uses_build_prefix(self):
        """Verify apply_retention passes the correct prefix to store.list."""
        listed_prefix = []

        class FakeStore:
            def list(self, prefix):
                listed_prefix.append(prefix)
                return []

        apply_retention(FakeStore(), "prod", "mydb", RetentionPolicy(keep_last=1))
        assert listed_prefix == ["prod/mydb"]


class TestRetentionEdgeCases:
    def test_all_five_rules_combined(self):
        """All 5 retention rules active simultaneously."""
        ref = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        # Create backups spanning 2 years (one per week)
        backups = []
        for i in range(104):  # ~2 years of weekly backups
            ts = ref - timedelta(days=i * 7)
            backups.append(BackupInfo(
                key=f"b{i}", filename=f"db-{ts.strftime('%Y%m%d-%H%M%S')}.sql.gz",
                timestamp=ts, size=1000,
            ))
        policy = RetentionPolicy(
            keep_last=3, keep_daily=7, keep_weekly=4,
            keep_monthly=6, keep_yearly=2,
        )
        keep = compute_keep_set(backups, policy, now=ref)
        # At minimum, keep_last keeps 3
        assert len(keep) >= 3
        # keep_yearly should keep at least 2 (2026 and 2025)
        assert "b0" in keep  # most recent

    def test_same_second_timestamps(self):
        """Multiple backups with identical timestamps → only one kept per bucket."""
        ref = datetime(2026, 2, 10, 12, 0, 0, tzinfo=timezone.utc)
        ts = ref
        backups = [
            BackupInfo(key="a", filename="db-20260210-120000.sql.gz", timestamp=ts, size=100),
            BackupInfo(key="b", filename="db-20260210-120000.sql.gz", timestamp=ts, size=200),
        ]
        policy = RetentionPolicy(keep_last=1)
        keep = compute_keep_set(backups, policy, now=ref)
        assert len(keep) == 1

    def test_keep_daily_at_midnight(self):
        """Backups exactly at midnight UTC → should be assigned to the correct day."""
        ref = datetime(2026, 2, 10, 0, 0, 0, tzinfo=timezone.utc)
        backups = [
            BackupInfo(key="midnight", filename="db-20260210-000000.sql.gz",
                       timestamp=datetime(2026, 2, 10, 0, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="prev_midnight", filename="db-20260209-000000.sql.gz",
                       timestamp=datetime(2026, 2, 9, 0, 0, 0, tzinfo=timezone.utc), size=100),
        ]
        policy = RetentionPolicy(keep_daily=2)
        keep = compute_keep_set(backups, policy, now=ref)
        assert keep == {"midnight", "prev_midnight"}

    def test_negative_retention_treated_as_zero(self):
        """Negative retention values are <= 0, so the rule is disabled."""
        backups = [_bi("b0", 0)]
        policy = RetentionPolicy(keep_last=-1)
        keep = compute_keep_set(backups, policy)
        assert keep == set()  # -1 is not > 0

    def test_very_large_keep_last(self):
        """Very large keep_last with few backups → keeps all."""
        backups = [_bi(f"b{i}", i) for i in range(3)]
        policy = RetentionPolicy(keep_last=999999)
        keep = compute_keep_set(backups, policy)
        assert keep == {"b0", "b1", "b2"}

    def test_apply_retention_delete_failure_propagates(self):
        """If store.delete() fails, the error propagates."""
        class FakeStore:
            def list(self, prefix):
                return [_bi("b0", 0), _bi("b1", 1)]
            def delete(self, key):
                raise RuntimeError("S3 permission denied")

        with pytest.raises(RuntimeError, match="S3 permission denied"):
            apply_retention(FakeStore(), "pfx", "db", RetentionPolicy(keep_last=1))

    def test_leap_year_monthly(self):
        """Monthly retention spanning Feb 28/29 in a leap year."""
        ref = datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        backups = [
            BackupInfo(key="mar", filename="db-20240315-120000.sql.gz",
                       timestamp=datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="feb", filename="db-20240229-120000.sql.gz",
                       timestamp=datetime(2024, 2, 29, 12, 0, 0, tzinfo=timezone.utc), size=100),
            BackupInfo(key="jan", filename="db-20240115-120000.sql.gz",
                       timestamp=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc), size=100),
        ]
        policy = RetentionPolicy(keep_monthly=2)
        keep = compute_keep_set(backups, policy, now=ref)
        assert keep == {"mar", "feb"}
