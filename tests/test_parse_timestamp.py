"""Tests for stores.parse_timestamp and is_backup_file."""

from datetime import datetime, timezone

from stores import parse_timestamp, is_backup_file


def test_standard_filename():
    ts = parse_timestamp("mydb-20260115-093000.sql.gz")
    assert ts == datetime(2026, 1, 15, 9, 30, 0, tzinfo=timezone.utc)


def test_database_name_with_hyphens():
    ts = parse_timestamp("my-app-db-20260210-143022.sql.gz")
    assert ts == datetime(2026, 2, 10, 14, 30, 22, tzinfo=timezone.utc)


def test_midnight():
    ts = parse_timestamp("db-20251231-000000.sql.gz")
    assert ts == datetime(2025, 12, 31, 0, 0, 0, tzinfo=timezone.utc)


def test_invalid_no_timestamp():
    assert parse_timestamp("random-file.sql.gz") is None


def test_invalid_bad_date():
    assert parse_timestamp("db-99999999-000000.sql.gz") is None


def test_invalid_bad_time():
    assert parse_timestamp("db-20260101-999999.sql.gz") is None


def test_invalid_no_extension():
    # Without .sql.gz suffix, the time part won't parse correctly
    assert parse_timestamp("db-20260101-120000.tar.gz") is None


def test_invalid_too_few_parts():
    assert parse_timestamp("backup.sql.gz") is None


def test_empty_string():
    assert parse_timestamp("") is None


def test_utc_timezone():
    ts = parse_timestamp("db-20260101-120000.sql.gz")
    assert ts.tzinfo == timezone.utc


def test_only_date_no_time():
    """Filename with only 2 hyphen-separated parts after removesuffix → None."""
    assert parse_timestamp("db-20260101.sql.gz") is None


def test_extra_chars_after_time():
    """Extra characters appended to time portion → should fail."""
    assert parse_timestamp("db-20260101-120000extra.sql.gz") is None


def test_wrong_extension_tar_bz2():
    assert parse_timestamp("db-20260101-120000.tar.bz2") is None


def test_leap_year_feb29():
    ts = parse_timestamp("db-20240229-120000.sql.gz")
    assert ts is not None
    assert ts.month == 2
    assert ts.day == 29


def test_non_leap_year_feb29():
    """Feb 29 in a non-leap year → invalid date → None."""
    assert parse_timestamp("db-20250229-120000.sql.gz") is None


# -- New extension tests --------------------------------------------------

def test_sql_zst_extension():
    ts = parse_timestamp("db-20260101-120000.sql.zst")
    assert ts == datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_sql_lz4_extension():
    ts = parse_timestamp("db-20260101-120000.sql.lz4")
    assert ts == datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_dump_gz_extension():
    ts = parse_timestamp("db-20260101-120000.dump.gz")
    assert ts == datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_dump_zst_extension():
    ts = parse_timestamp("db-20260101-120000.dump.zst")
    assert ts == datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_dump_lz4_extension():
    ts = parse_timestamp("db-20260101-120000.dump.lz4")
    assert ts == datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_plain_sql_extension():
    ts = parse_timestamp("db-20260101-120000.sql")
    assert ts == datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_plain_dump_extension():
    ts = parse_timestamp("db-20260101-120000.dump")
    assert ts == datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


# -- is_backup_file tests ------------------------------------------------

def test_is_backup_file_recognized():
    assert is_backup_file("db-20260101-120000.sql.gz")
    assert is_backup_file("db-20260101-120000.sql.zst")
    assert is_backup_file("db-20260101-120000.sql.lz4")
    assert is_backup_file("db-20260101-120000.sql")
    assert is_backup_file("db-20260101-120000.dump.gz")
    assert is_backup_file("db-20260101-120000.dump.zst")
    assert is_backup_file("db-20260101-120000.dump.lz4")
    assert is_backup_file("db-20260101-120000.dump")


def test_is_backup_file_unrecognized():
    assert not is_backup_file("readme.txt")
    assert not is_backup_file("backup.tar.gz")
    assert not is_backup_file("data.csv")
    assert not is_backup_file("")
