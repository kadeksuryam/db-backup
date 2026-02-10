"""Tests for utils module."""

from __future__ import annotations

import hashlib

from utils import sha256_file


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
