"""Tests for utils module."""

from __future__ import annotations

import hashlib

from utils import format_size, sha256_file


class TestSha256File:
    def test_basic_file_hash(self, tmp_path):
        """Hash of a file with known content matches hashlib directly."""
        f = tmp_path / "test.bin"
        content = b"hello world\n"
        f.write_bytes(content)
        expected = hashlib.sha256(content).hexdigest()
        assert sha256_file(str(f)) == expected

    def test_empty_file_hash(self, tmp_path):
        """Hash of an empty file matches hashlib's hash of empty bytes."""
        f = tmp_path / "empty.bin"
        f.write_bytes(b"")
        expected = hashlib.sha256(b"").hexdigest()
        assert sha256_file(str(f)) == expected


class TestFormatSize:
    def test_bytes(self):
        assert format_size(0) == "0 B"
        assert format_size(500) == "500 B"
        assert format_size(1023) == "1023 B"

    def test_kilobytes(self):
        assert format_size(1024) == "1.0 KB"
        assert format_size(2048) == "2.0 KB"

    def test_megabytes(self):
        assert format_size(1024 * 1024) == "1.0 MB"
        assert format_size(5 * 1024 * 1024) == "5.0 MB"

    def test_gigabytes(self):
        assert format_size(1024 ** 3) == "1.0 GB"
        assert format_size(2 * 1024 ** 3) == "2.0 GB"
