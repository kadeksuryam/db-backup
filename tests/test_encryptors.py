"""Tests for encryptors package — factory, AgeEncryptor, GPGEncryptor, AES256GCMEncryptor."""

from __future__ import annotations

import os
import struct
from unittest.mock import MagicMock, patch, ANY

import pytest

from config import ConfigError
from encryptors import create_encryptor, _ENCRYPTOR_TYPES
from encryptors.aes256gcm import AES256GCMEncryptor, MAGIC, NONCE_SIZE


class TestCreateEncryptor:
    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_creates_age(self, mock_which):
        enc = create_encryptor({
            "type": "age",
            "recipients": ["age1testkey"],
        })
        from encryptors.age import AgeEncryptor
        assert isinstance(enc, AgeEncryptor)

    @patch("encryptors.gpg.shutil.which", return_value="/usr/bin/gpg")
    def test_creates_gpg(self, mock_which):
        enc = create_encryptor({
            "type": "gpg",
            "key_id": "0xABCD1234",
        })
        from encryptors.gpg import GPGEncryptor
        assert isinstance(enc, GPGEncryptor)

    def test_creates_aes256gcm(self):
        key_hex = "a" * 64  # 32 bytes
        enc = create_encryptor({
            "type": "aes-256-gcm",
            "key": key_hex,
        })
        assert isinstance(enc, AES256GCMEncryptor)

    def test_unknown_type_raises(self):
        with pytest.raises(ConfigError, match="Unknown encryptor type"):
            create_encryptor({"type": "rot13"})

    def test_missing_type_raises(self):
        with pytest.raises(ConfigError, match="Unknown encryptor type"):
            create_encryptor({})

    def test_all_types_registered(self):
        assert "age" in _ENCRYPTOR_TYPES
        assert "gpg" in _ENCRYPTOR_TYPES
        assert "aes-256-gcm" in _ENCRYPTOR_TYPES


class TestAgeEncryptor:
    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_missing_binary_raises(self, mock_which):
        mock_which.return_value = None
        with pytest.raises(ConfigError, match="not found in PATH"):
            create_encryptor({"type": "age", "recipients": ["age1test"]})

    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_no_recipients_or_passphrase_raises(self, mock_which):
        with pytest.raises(ConfigError, match="requires either"):
            create_encryptor({"type": "age"})

    @patch("encryptors.age.subprocess.run")
    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_encrypt_recipient_mode(self, mock_which, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        enc = create_encryptor({
            "type": "age",
            "recipients": ["age1testkey"],
        })
        enc.encrypt("/tmp/in.sql.gz", "/tmp/out.sql.gz.age")
        cmd = mock_run.call_args[0][0]
        assert "--encrypt" in cmd
        assert "--recipient" in cmd
        assert "age1testkey" in cmd

    @patch("encryptors.age.subprocess.run")
    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_encrypt_passphrase_mode(self, mock_which, mock_run, monkeypatch):
        monkeypatch.setenv("AGE_PASS", "mysecret")
        mock_run.return_value = MagicMock(returncode=0)
        enc = create_encryptor({
            "type": "age",
            "passphrase_env": "AGE_PASS",
        })
        enc.encrypt("/tmp/in", "/tmp/out")
        cmd = mock_run.call_args[0][0]
        assert "--passphrase" in cmd
        env = mock_run.call_args[1]["env"]
        assert env["AGE_PASSPHRASE"] == "mysecret"

    @patch("encryptors.age.subprocess.run")
    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_decrypt_identity_mode(self, mock_which, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        enc = create_encryptor({
            "type": "age",
            "recipients": ["age1testkey"],
            "identity": "/keys/key.txt",
        })
        enc.decrypt("/tmp/in.age", "/tmp/out")
        cmd = mock_run.call_args[0][0]
        assert "--decrypt" in cmd
        assert "--identity" in cmd
        assert "/keys/key.txt" in cmd

    @patch("encryptors.age.subprocess.run")
    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_encrypt_failure_raises(self, mock_which, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stderr="age error")
        enc = create_encryptor({
            "type": "age",
            "recipients": ["age1testkey"],
        })
        with pytest.raises(RuntimeError, match="age encrypt failed"):
            enc.encrypt("/tmp/in", "/tmp/out")

    @patch("encryptors.age.subprocess.run")
    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_decrypt_failure_raises(self, mock_which, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stderr="decrypt error")
        enc = create_encryptor({
            "type": "age",
            "recipients": ["age1testkey"],
            "identity": "/keys/key.txt",
        })
        with pytest.raises(RuntimeError, match="age decrypt failed"):
            enc.decrypt("/tmp/in", "/tmp/out")

    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_file_suffix(self, mock_which):
        enc = create_encryptor({"type": "age", "recipients": ["age1test"]})
        assert enc.file_suffix() == ".age"

    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_singular_recipient_key(self, mock_which):
        """'recipient' (singular) is also accepted."""
        enc = create_encryptor({"type": "age", "recipient": "age1single"})
        assert enc._recipients == ["age1single"]

    @patch("encryptors.age.subprocess.run")
    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_passphrase_env_not_set_encrypt_raises(self, mock_which, mock_run):
        """If passphrase env var is not set at encrypt time, raises."""
        enc = create_encryptor({
            "type": "age",
            "passphrase_env": "NONEXISTENT_VAR",
        })
        with pytest.raises(RuntimeError, match="not set"):
            enc.encrypt("/tmp/in", "/tmp/out")

    @patch("encryptors.age.subprocess.run")
    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_passphrase_env_not_set_decrypt_raises(self, mock_which, mock_run):
        """If passphrase env var is not set at decrypt time, raises."""
        enc = create_encryptor({
            "type": "age",
            "passphrase_env": "NONEXISTENT_VAR",
        })
        with pytest.raises(RuntimeError, match="not set"):
            enc.decrypt("/tmp/in", "/tmp/out")

    @patch("encryptors.age.subprocess.run")
    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_decrypt_passphrase_mode(self, mock_which, mock_run, monkeypatch):
        """Decrypt with passphrase_env uses AGE_PASSPHRASE in env."""
        monkeypatch.setenv("AGE_PASS", "mysecret")
        mock_run.return_value = MagicMock(returncode=0)
        enc = create_encryptor({
            "type": "age",
            "passphrase_env": "AGE_PASS",
        })
        enc.decrypt("/tmp/in.age", "/tmp/out")
        env = mock_run.call_args[1]["env"]
        assert env["AGE_PASSPHRASE"] == "mysecret"
        cmd = mock_run.call_args[0][0]
        assert "--decrypt" in cmd

    @patch("encryptors.age.subprocess.run")
    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_multiple_recipients_all_in_command(self, mock_which, mock_run):
        """All recipients appear as --recipient flags."""
        mock_run.return_value = MagicMock(returncode=0)
        enc = create_encryptor({
            "type": "age",
            "recipients": ["age1key1", "age1key2", "age1key3"],
        })
        enc.encrypt("/tmp/in", "/tmp/out")
        cmd = mock_run.call_args[0][0]
        assert cmd.count("--recipient") == 3
        assert "age1key1" in cmd
        assert "age1key2" in cmd
        assert "age1key3" in cmd

    @patch("encryptors.age.subprocess.run")
    @patch("encryptors.age.shutil.which", return_value="/usr/bin/age")
    def test_empty_passphrase_accepted(self, mock_which, mock_run, monkeypatch):
        """Empty string passphrase is set but empty — should not raise (age will reject it)."""
        monkeypatch.setenv("AGE_PASS", "")
        mock_run.return_value = MagicMock(returncode=0)
        enc = create_encryptor({
            "type": "age",
            "passphrase_env": "AGE_PASS",
        })
        # Should not raise — the env var IS set (just empty)
        enc.encrypt("/tmp/in", "/tmp/out")
        env = mock_run.call_args[1]["env"]
        assert env["AGE_PASSPHRASE"] == ""


class TestGPGEncryptor:
    @patch("encryptors.gpg.shutil.which", return_value="/usr/bin/gpg")
    def test_missing_binary_raises(self, mock_which):
        mock_which.return_value = None
        with pytest.raises(ConfigError, match="not found in PATH"):
            create_encryptor({"type": "gpg", "key_id": "0xABCD"})

    @patch("encryptors.gpg.shutil.which", return_value="/usr/bin/gpg")
    def test_no_key_or_passphrase_raises(self, mock_which):
        with pytest.raises(ConfigError, match="requires either"):
            create_encryptor({"type": "gpg"})

    @patch("encryptors.gpg.subprocess.run")
    @patch("encryptors.gpg.shutil.which", return_value="/usr/bin/gpg")
    def test_encrypt_recipient_mode(self, mock_which, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        enc = create_encryptor({"type": "gpg", "key_id": "0xABCD"})
        enc.encrypt("/tmp/in", "/tmp/out")
        cmd = mock_run.call_args[0][0]
        assert "--encrypt" in cmd
        assert "--recipient" in cmd
        assert "0xABCD" in cmd

    @patch("encryptors.gpg.subprocess.run")
    @patch("encryptors.gpg.shutil.which", return_value="/usr/bin/gpg")
    def test_encrypt_symmetric_mode(self, mock_which, mock_run, monkeypatch):
        monkeypatch.setenv("GPG_PASS", "secret")
        mock_run.return_value = MagicMock(returncode=0)
        enc = create_encryptor({"type": "gpg", "passphrase_env": "GPG_PASS"})
        enc.encrypt("/tmp/in", "/tmp/out")
        cmd = mock_run.call_args[0][0]
        assert "--symmetric" in cmd
        assert "--cipher-algo" in cmd
        assert "AES256" in cmd

    @patch("encryptors.gpg.subprocess.run")
    @patch("encryptors.gpg.shutil.which", return_value="/usr/bin/gpg")
    def test_decrypt(self, mock_which, mock_run):
        mock_run.return_value = MagicMock(returncode=0)
        enc = create_encryptor({"type": "gpg", "key_id": "0xABCD"})
        enc.decrypt("/tmp/in.gpg", "/tmp/out")
        cmd = mock_run.call_args[0][0]
        assert "--decrypt" in cmd
        assert "--batch" in cmd

    @patch("encryptors.gpg.subprocess.run")
    @patch("encryptors.gpg.shutil.which", return_value="/usr/bin/gpg")
    def test_encrypt_failure_raises(self, mock_which, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stderr="gpg error")
        enc = create_encryptor({"type": "gpg", "key_id": "0xABCD"})
        with pytest.raises(RuntimeError, match="gpg encrypt failed"):
            enc.encrypt("/tmp/in", "/tmp/out")

    @patch("encryptors.gpg.subprocess.run")
    @patch("encryptors.gpg.shutil.which", return_value="/usr/bin/gpg")
    def test_decrypt_failure_raises(self, mock_which, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stderr="bad passphrase")
        enc = create_encryptor({"type": "gpg", "key_id": "0xABCD"})
        with pytest.raises(RuntimeError, match="gpg decrypt failed"):
            enc.decrypt("/tmp/in", "/tmp/out")

    @patch("encryptors.gpg.shutil.which", return_value="/usr/bin/gpg")
    def test_file_suffix(self, mock_which):
        enc = create_encryptor({"type": "gpg", "key_id": "0xABCD"})
        assert enc.file_suffix() == ".gpg"

    @patch("encryptors.gpg.subprocess.run")
    @patch("encryptors.gpg.shutil.which", return_value="/usr/bin/gpg")
    def test_symmetric_passphrase_not_set_encrypt_raises(self, mock_which, mock_run):
        enc = create_encryptor({"type": "gpg", "passphrase_env": "NONEXISTENT"})
        with pytest.raises(RuntimeError, match="not set"):
            enc.encrypt("/tmp/in", "/tmp/out")

    @patch("encryptors.gpg.subprocess.run")
    @patch("encryptors.gpg.shutil.which", return_value="/usr/bin/gpg")
    def test_symmetric_passphrase_not_set_decrypt_raises(self, mock_which, mock_run):
        """Decrypt with passphrase_env not set raises."""
        enc = create_encryptor({"type": "gpg", "passphrase_env": "NONEXISTENT"})
        with pytest.raises(RuntimeError, match="not set"):
            enc.decrypt("/tmp/in", "/tmp/out")

    @patch("encryptors.gpg.subprocess.run")
    @patch("encryptors.gpg.shutil.which", return_value="/usr/bin/gpg")
    def test_decrypt_with_passphrase(self, mock_which, mock_run, monkeypatch):
        """Decrypt with passphrase_env passes --passphrase-fd 0 and pipes passphrase."""
        monkeypatch.setenv("GPG_PASS", "secret")
        mock_run.return_value = MagicMock(returncode=0)
        enc = create_encryptor({"type": "gpg", "passphrase_env": "GPG_PASS"})
        enc.decrypt("/tmp/in.gpg", "/tmp/out")
        cmd = mock_run.call_args[0][0]
        assert "--passphrase-fd" in cmd
        assert mock_run.call_args[1]["input"] == "secret"


class TestAES256GCMEncryptor:
    def _key(self) -> bytes:
        return bytes(range(32))

    def _key_hex(self) -> str:
        return self._key().hex()

    def test_roundtrip(self, tmp_path):
        """Encrypt then decrypt should return original data."""
        key = self._key()
        enc = AES256GCMEncryptor(key)

        original = b"Hello, this is test data for AES-256-GCM encryption!"
        input_file = tmp_path / "plaintext.bin"
        input_file.write_bytes(original)

        encrypted_file = tmp_path / "encrypted.enc"
        enc.encrypt(str(input_file), str(encrypted_file))

        # Encrypted file should differ from original
        assert encrypted_file.read_bytes() != original
        # Should start with magic
        assert encrypted_file.read_bytes().startswith(MAGIC)

        decrypted_file = tmp_path / "decrypted.bin"
        enc.decrypt(str(encrypted_file), str(decrypted_file))

        assert decrypted_file.read_bytes() == original

    def test_roundtrip_large_file(self, tmp_path):
        """Roundtrip with data larger than one chunk to test chunked processing."""
        key = self._key()
        enc = AES256GCMEncryptor(key)

        # Use a small chunk size for testing
        import encryptors.aes256gcm as mod
        original_chunk_size = mod.CHUNK_SIZE
        mod.CHUNK_SIZE = 1024  # 1 KB chunks

        try:
            original = os.urandom(5000)  # ~5 chunks
            input_file = tmp_path / "large.bin"
            input_file.write_bytes(original)

            encrypted_file = tmp_path / "large.enc"
            enc.encrypt(str(input_file), str(encrypted_file))

            decrypted_file = tmp_path / "large.dec"
            enc.decrypt(str(encrypted_file), str(decrypted_file))

            assert decrypted_file.read_bytes() == original
        finally:
            mod.CHUNK_SIZE = original_chunk_size

    def test_wrong_key_fails(self, tmp_path):
        """Decrypt with wrong key should fail."""
        key1 = bytes(range(32))
        key2 = bytes(range(1, 33))

        enc1 = AES256GCMEncryptor(key1)
        enc2 = AES256GCMEncryptor(key2)

        input_file = tmp_path / "plain.bin"
        input_file.write_bytes(b"secret data")

        encrypted_file = tmp_path / "encrypted.enc"
        enc1.encrypt(str(input_file), str(encrypted_file))

        decrypted_file = tmp_path / "decrypted.bin"
        with pytest.raises(Exception):  # cryptography raises InvalidTag
            enc2.decrypt(str(encrypted_file), str(decrypted_file))

    def test_invalid_key_length_raises(self):
        with pytest.raises(ConfigError, match="exactly 32 bytes"):
            AES256GCMEncryptor(b"short")

    def test_invalid_magic_raises(self, tmp_path):
        encrypted_file = tmp_path / "bad.enc"
        encrypted_file.write_bytes(b"NOT_A_VALID_FILE")
        decrypted_file = tmp_path / "out.bin"

        enc = AES256GCMEncryptor(self._key())
        with pytest.raises(RuntimeError, match="magic header mismatch"):
            enc.decrypt(str(encrypted_file), str(decrypted_file))

    def test_truncated_nonce_raises(self, tmp_path):
        encrypted_file = tmp_path / "truncated.enc"
        encrypted_file.write_bytes(MAGIC + b"\x00" * 5)  # too short nonce
        decrypted_file = tmp_path / "out.bin"

        enc = AES256GCMEncryptor(self._key())
        with pytest.raises(RuntimeError, match="truncated base nonce"):
            enc.decrypt(str(encrypted_file), str(decrypted_file))

    def test_file_suffix(self):
        enc = AES256GCMEncryptor(self._key())
        assert enc.file_suffix() == ".enc"

    def test_factory_with_hex_key(self):
        enc = create_encryptor({
            "type": "aes-256-gcm",
            "key": "a" * 64,
        })
        assert isinstance(enc, AES256GCMEncryptor)

    def test_factory_invalid_hex_raises(self):
        with pytest.raises(ConfigError, match="not valid hex"):
            create_encryptor({
                "type": "aes-256-gcm",
                "key": "not-hex",
            })

    def test_factory_no_key_raises(self):
        with pytest.raises(ConfigError, match="requires either"):
            create_encryptor({"type": "aes-256-gcm"})

    def test_factory_with_key_file(self, tmp_path):
        key_file = tmp_path / "key.hex"
        key_file.write_text("a" * 64)
        enc = create_encryptor({
            "type": "aes-256-gcm",
            "key_file": str(key_file),
        })
        assert isinstance(enc, AES256GCMEncryptor)

    def test_factory_key_file_not_found_raises(self):
        with pytest.raises(ConfigError, match="key file not found"):
            create_encryptor({
                "type": "aes-256-gcm",
                "key_file": "/nonexistent/key.hex",
            })

    def test_empty_file_roundtrip(self, tmp_path):
        """Encrypting an empty file should roundtrip cleanly."""
        enc = AES256GCMEncryptor(self._key())

        input_file = tmp_path / "empty.bin"
        input_file.write_bytes(b"")

        encrypted_file = tmp_path / "empty.enc"
        enc.encrypt(str(input_file), str(encrypted_file))

        decrypted_file = tmp_path / "empty.dec"
        enc.decrypt(str(encrypted_file), str(decrypted_file))

        assert decrypted_file.read_bytes() == b""

    def test_truncated_chunk_length_raises(self, tmp_path):
        """File with truncated chunk length (< 4 bytes after header) raises."""
        enc = AES256GCMEncryptor(self._key())
        bad_file = tmp_path / "bad.enc"
        # Valid header + valid nonce + incomplete chunk length (2 bytes instead of 4)
        bad_file.write_bytes(MAGIC + b"\x00" * NONCE_SIZE + b"\x00\x01")
        out = tmp_path / "out.bin"
        with pytest.raises(RuntimeError, match="truncated chunk length"):
            enc.decrypt(str(bad_file), str(out))

    def test_truncated_chunk_data_raises(self, tmp_path):
        """File with truncated chunk data raises."""
        enc = AES256GCMEncryptor(self._key())
        bad_file = tmp_path / "bad.enc"
        # Valid header + nonce + chunk_len says 100 bytes but only 5 follow
        bad_file.write_bytes(
            MAGIC + b"\x00" * NONCE_SIZE +
            struct.pack(">I", 100) + b"\x00" * 5
        )
        out = tmp_path / "out.bin"
        with pytest.raises(RuntimeError, match="truncated chunk data"):
            enc.decrypt(str(bad_file), str(out))

    def test_corrupted_chunk_nonce_raises(self, tmp_path):
        """Tampered nonce in chunk raises nonce mismatch."""
        enc = AES256GCMEncryptor(self._key())

        # Encrypt a valid file first
        input_file = tmp_path / "plain.bin"
        input_file.write_bytes(b"test data for corruption test")
        encrypted_file = tmp_path / "encrypted.enc"
        enc.encrypt(str(input_file), str(encrypted_file))

        # Corrupt the nonce in the first chunk (bytes right after the 4-byte length)
        data = bytearray(encrypted_file.read_bytes())
        header_len = len(MAGIC) + NONCE_SIZE + 4  # magic + base_nonce + chunk_len
        # Flip a byte in the nonce area
        data[header_len] ^= 0xFF
        corrupted_file = tmp_path / "corrupted.enc"
        corrupted_file.write_bytes(bytes(data))

        out = tmp_path / "out.bin"
        with pytest.raises(RuntimeError, match="nonce mismatch"):
            enc.decrypt(str(corrupted_file), str(out))

    def test_factory_with_raw_binary_key_file(self, tmp_path):
        """Key file with raw 32-byte binary key (not hex-encoded)."""
        key = os.urandom(32)
        key_file = tmp_path / "key.bin"
        key_file.write_bytes(key)
        enc = create_encryptor({
            "type": "aes-256-gcm",
            "key_file": str(key_file),
        })
        assert isinstance(enc, AES256GCMEncryptor)
        assert enc._key == key

    def test_factory_raw_binary_key_not_stripped(self, tmp_path):
        """Raw binary key starting/ending with whitespace-like bytes is preserved."""
        # Key that starts with \n and ends with \t — should NOT be stripped
        key = b"\n" + os.urandom(30) + b"\t"
        assert len(key) == 32
        key_file = tmp_path / "key.bin"
        key_file.write_bytes(key)
        enc = create_encryptor({
            "type": "aes-256-gcm",
            "key_file": str(key_file),
        })
        assert enc._key == key

    def test_factory_hex_key_file_with_newline(self, tmp_path):
        """Hex-encoded key file with trailing newline is handled correctly."""
        key_hex = "a" * 64
        key_file = tmp_path / "key.hex"
        key_file.write_text(key_hex + "\n")
        enc = create_encryptor({
            "type": "aes-256-gcm",
            "key_file": str(key_file),
        })
        assert isinstance(enc, AES256GCMEncryptor)
        assert enc._key == bytes.fromhex(key_hex)


class TestBackupWithEncryption:
    """Integration tests: backup.py + encryption."""

    @patch("backup.create_encryptor")
    @patch("backup.create_engine")
    def test_backup_encrypts_after_dump(self, mock_create_engine, mock_create_enc):
        """Encryption step runs after dump, plaintext is deleted."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"dump data")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_encryptor = MagicMock()
        mock_encryptor.file_suffix.return_value = ".age"

        def fake_encrypt(inp, out):
            with open(out, "wb") as f:
                f.write(b"encrypted data")

        mock_encryptor.encrypt.side_effect = fake_encrypt
        mock_create_enc.return_value = mock_encryptor

        mock_store = MagicMock()

        from config import Datasource
        from backup import run_backup
        ds = Datasource(
            name="test", engine="postgres", host="localhost",
            port=5432, user="u", password="p", database="testdb",
        )
        enc_cfg = {"type": "age", "recipients": ["age1test"]}
        key = run_backup(ds, mock_store, "prod", encryption_config=enc_cfg)

        # Key should end with encryption suffix
        assert key.endswith(".sql.gz.age")
        # Encryptor was used
        mock_encryptor.encrypt.assert_called_once()
        # SHA256 is of the encrypted file
        assert mock_store.upload.call_count == 2

    @patch("backup.create_encryptor")
    @patch("backup.create_engine")
    def test_backup_encryption_failure_skips_upload(self, mock_create_engine, mock_create_enc):
        """If encryption fails, upload is not called."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"dump data")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_encryptor = MagicMock()
        mock_encryptor.file_suffix.return_value = ".age"
        mock_encryptor.encrypt.side_effect = RuntimeError("encryption failed")
        mock_create_enc.return_value = mock_encryptor

        mock_store = MagicMock()

        from config import Datasource
        from backup import run_backup
        ds = Datasource(
            name="test", engine="postgres", host="localhost",
            port=5432, user="u", password="p", database="testdb",
        )
        with pytest.raises(RuntimeError, match="encryption failed"):
            run_backup(ds, mock_store, "prod", encryption_config={"type": "age"})

        mock_store.upload.assert_not_called()

    @patch("backup.create_encryptor")
    @patch("backup.create_engine")
    def test_backup_verify_with_encryption(self, mock_create_engine, mock_create_enc):
        """verify=True + encryption: download → decrypt → engine.verify."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"dump data")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_encryptor = MagicMock()
        mock_encryptor.file_suffix.return_value = ".age"

        def fake_encrypt(inp, out):
            with open(out, "wb") as f:
                f.write(b"encrypted")

        def fake_decrypt(inp, out):
            with open(out, "wb") as f:
                f.write(b"decrypted for verify")

        mock_encryptor.encrypt.side_effect = fake_encrypt
        mock_encryptor.decrypt.side_effect = fake_decrypt
        mock_create_enc.return_value = mock_encryptor

        mock_store = MagicMock()

        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"encrypted download")

        mock_store.download.side_effect = fake_download

        from config import Datasource
        from backup import run_backup
        ds = Datasource(
            name="test", engine="postgres", host="localhost",
            port=5432, user="u", password="p", database="testdb",
        )
        run_backup(ds, mock_store, "prod", verify=True,
                   encryption_config={"type": "age"})

        # encrypt called for backup, decrypt called for verification
        mock_encryptor.encrypt.assert_called_once()
        mock_encryptor.decrypt.assert_called_once()
        mock_engine.verify.assert_called_once()

    @patch("backup.create_encryptor")
    @patch("backup.create_engine")
    def test_backup_sha256_is_of_encrypted_file(self, mock_create_engine, mock_create_enc):
        """SHA256 sidecar is computed on the encrypted file, not plaintext."""
        import hashlib
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"plaintext dump")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        encrypted_data = b"encrypted content here"
        mock_encryptor = MagicMock()
        mock_encryptor.file_suffix.return_value = ".age"

        def fake_encrypt(inp, out):
            with open(out, "wb") as f:
                f.write(encrypted_data)

        mock_encryptor.encrypt.side_effect = fake_encrypt
        mock_create_enc.return_value = mock_encryptor

        uploaded_files = {}

        def capture_upload(local_path, remote_key):
            with open(local_path, "rb") as f:
                uploaded_files[remote_key] = f.read()

        mock_store = MagicMock()
        mock_store.upload.side_effect = capture_upload

        from config import Datasource
        from backup import run_backup
        ds = Datasource(
            name="test", engine="postgres", host="localhost",
            port=5432, user="u", password="p", database="testdb",
        )
        key = run_backup(ds, mock_store, "prod", encryption_config={"type": "age"})

        # SHA256 sidecar content should match hash of encrypted data
        sidecar = uploaded_files[key + ".sha256"].decode().strip()
        expected = hashlib.sha256(encrypted_data).hexdigest()
        assert sidecar == expected

    @patch("backup.create_engine")
    def test_backup_no_encryption_unchanged(self, mock_create_engine):
        """Without encryption_config, backup works as before."""
        mock_engine = MagicMock()
        mock_engine.file_extension.return_value = ".sql.gz"

        def fake_dump(ds, output_path):
            with open(output_path, "wb") as f:
                f.write(b"data")

        mock_engine.dump.side_effect = fake_dump
        mock_create_engine.return_value = mock_engine

        mock_store = MagicMock()

        from config import Datasource
        from backup import run_backup
        ds = Datasource(
            name="test", engine="postgres", host="localhost",
            port=5432, user="u", password="p", database="testdb",
        )
        key = run_backup(ds, mock_store, "prod")

        assert key.endswith(".sql.gz")
        assert not key.endswith(".age")


class TestRestoreWithEncryption:
    """Integration tests: restore.py + encryption."""

    @patch("restore.create_encryptor")
    @patch("restore.create_engine")
    def test_restore_decrypts_before_verify(self, mock_create_engine, mock_create_enc):
        """Decryption happens after download+checksum, before verify+restore."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 0
        mock_create_engine.return_value = mock_engine

        mock_encryptor = MagicMock()
        mock_encryptor.file_suffix.return_value = ".age"

        call_order = []

        def fake_decrypt(inp, out):
            call_order.append("decrypt")
            with open(out, "wb") as f:
                f.write(b"decrypted data")
        mock_encryptor.decrypt.side_effect = fake_decrypt
        mock_create_enc.return_value = mock_encryptor

        mock_engine.verify.side_effect = lambda ds, path: call_order.append("verify")
        mock_engine.restore.side_effect = lambda ds, path: call_order.append("restore")

        from datetime import datetime, timezone
        from stores import BackupInfo

        store = MagicMock()
        store.list.return_value = [
            BackupInfo(
                key="prod/testdb/db-20260102-120000.sql.gz.age",
                filename="db-20260102-120000.sql.gz.age",
                timestamp=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
                size=1000,
            ),
        ]

        def fake_download(key, path):
            call_order.append("download")
            with open(path, "wb") as f:
                f.write(b"encrypted data")
        store.download.side_effect = fake_download

        from config import Datasource
        from restore import run_restore
        ds = Datasource(
            name="test", engine="postgres", host="localhost",
            port=5432, user="u", password="p", database="testdb",
        )
        enc_cfg = {"type": "age", "recipients": ["age1test"], "identity": "/keys/key.txt"}
        run_restore(ds, store, "prod", encryption_config=enc_cfg)

        # Order: download → decrypt → verify → restore
        assert call_order[0] == "download"
        assert "decrypt" in call_order
        decrypt_idx = call_order.index("decrypt")
        verify_idx = call_order.index("verify")
        restore_idx = call_order.index("restore")
        assert decrypt_idx < verify_idx < restore_idx

    @patch("restore.create_encryptor")
    @patch("restore.create_engine")
    def test_restore_decryption_failure_prevents_restore(self, mock_create_engine, mock_create_enc):
        """If decryption fails, DB is not touched."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 5
        mock_create_engine.return_value = mock_engine

        mock_encryptor = MagicMock()
        mock_encryptor.file_suffix.return_value = ".age"
        mock_encryptor.decrypt.side_effect = RuntimeError("decryption failed")
        mock_create_enc.return_value = mock_encryptor

        from datetime import datetime, timezone
        from stores import BackupInfo

        store = MagicMock()
        store.list.return_value = [
            BackupInfo(
                key="prod/testdb/db-20260102-120000.sql.gz.age",
                filename="db-20260102-120000.sql.gz.age",
                timestamp=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
                size=1000,
            ),
        ]

        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"encrypted data")
        store.download.side_effect = fake_download

        from config import Datasource
        from restore import run_restore
        ds = Datasource(
            name="test", engine="postgres", host="localhost",
            port=5432, user="u", password="p", database="testdb",
        )
        with pytest.raises(RuntimeError, match="decryption failed"):
            run_restore(ds, store, "prod", encryption_config={"type": "age"})

        mock_engine.drop_and_recreate.assert_not_called()
        mock_engine.restore.assert_not_called()
        mock_engine.verify.assert_not_called()

    @patch("restore.create_encryptor")
    @patch("restore.create_engine")
    def test_restore_with_encryption_and_checksum(self, mock_create_engine, mock_create_enc):
        """Full path: download → SHA256 check → decrypt → verify → restore."""
        import hashlib

        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 0
        mock_create_engine.return_value = mock_engine

        encrypted_data = b"encrypted backup content"
        checksum = hashlib.sha256(encrypted_data).hexdigest()

        mock_encryptor = MagicMock()
        mock_encryptor.file_suffix.return_value = ".age"

        def fake_decrypt(inp, out):
            with open(out, "wb") as f:
                f.write(b"decrypted")
        mock_encryptor.decrypt.side_effect = fake_decrypt
        mock_create_enc.return_value = mock_encryptor

        from datetime import datetime, timezone
        from stores import BackupInfo

        store = MagicMock()
        store.list.return_value = [
            BackupInfo(
                key="prod/testdb/db-20260102-120000.sql.gz.age",
                filename="db-20260102-120000.sql.gz.age",
                timestamp=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
                size=1000,
            ),
        ]

        def fake_download(key, path):
            if key.endswith(".sha256"):
                with open(path, "w") as f:
                    f.write(checksum)
            else:
                with open(path, "wb") as f:
                    f.write(encrypted_data)
        store.download.side_effect = fake_download

        from config import Datasource
        from restore import run_restore
        ds = Datasource(
            name="test", engine="postgres", host="localhost",
            port=5432, user="u", password="p", database="testdb",
        )
        run_restore(ds, store, "prod", encryption_config={"type": "age"})

        mock_encryptor.decrypt.assert_called_once()
        mock_engine.verify.assert_called_once()
        mock_engine.restore.assert_called_once()

    @patch("restore.create_encryptor")
    @patch("restore.create_engine")
    def test_restore_wrong_suffix_raises(self, mock_create_engine, mock_create_enc):
        """If backup filename doesn't have expected encryption suffix, raises."""
        mock_engine = MagicMock()
        mock_engine.count_tables.return_value = 0
        mock_create_engine.return_value = mock_engine

        mock_encryptor = MagicMock()
        mock_encryptor.file_suffix.return_value = ".age"
        mock_create_enc.return_value = mock_encryptor

        from datetime import datetime, timezone
        from stores import BackupInfo

        store = MagicMock()
        store.list.return_value = [
            BackupInfo(
                key="prod/testdb/db-20260102-120000.sql.gz",
                filename="db-20260102-120000.sql.gz",
                timestamp=datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc),
                size=1000,
            ),
        ]

        def fake_download(key, path):
            with open(path, "wb") as f:
                f.write(b"data")
        store.download.side_effect = fake_download

        from config import Datasource
        from restore import run_restore
        ds = Datasource(
            name="test", engine="postgres", host="localhost",
            port=5432, user="u", password="p", database="testdb",
        )
        enc_cfg = {"type": "age", "recipients": ["age1test"]}
        with pytest.raises(RuntimeError, match="does not have expected encryption suffix"):
            run_restore(ds, store, "prod", encryption_config=enc_cfg)
