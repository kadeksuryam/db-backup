"""AES-256-GCM encryption backend (pure Python, no external binary)."""

from __future__ import annotations

import logging
import os
import struct

from config import ConfigError
from encryptors import Encryptor

log = logging.getLogger(__name__)

MAGIC = b"DBBACKUP_AES256GCM_V1\n"
CHUNK_SIZE = 64 * 1024 * 1024  # 64 MB
NONCE_SIZE = 12  # AES-GCM standard nonce size


class AES256GCMEncryptor(Encryptor):
    """Encrypt/decrypt using AES-256-GCM with chunked processing for large files.

    File format:
        MAGIC (22 bytes) + base_nonce (12 bytes) +
        [chunk_len (4 bytes, big-endian) + nonce (12 bytes) + ciphertext+tag (N+16 bytes)] ...
    """

    def __init__(self, key: bytes):
        if len(key) != 32:
            raise ConfigError(
                f"AES-256-GCM key must be exactly 32 bytes, got {len(key)}"
            )
        self._key = key

    def encrypt(self, input_path: str, output_path: str) -> None:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        aesgcm = AESGCM(self._key)
        base_nonce = os.urandom(NONCE_SIZE)

        log.info("Encrypting with AES-256-GCM: %s -> %s", input_path, output_path)

        with open(input_path, "rb") as fin, open(output_path, "wb") as fout:
            fout.write(MAGIC)
            fout.write(base_nonce)

            chunk_index = 0
            while True:
                chunk = fin.read(CHUNK_SIZE)
                if not chunk:
                    break

                nonce = _derive_nonce(base_nonce, chunk_index)
                ciphertext = aesgcm.encrypt(nonce, chunk, None)

                # Write: chunk_len (length of nonce + ciphertext) + nonce + ciphertext
                segment = nonce + ciphertext
                fout.write(struct.pack(">I", len(segment)))
                fout.write(segment)

                chunk_index += 1

    def decrypt(self, input_path: str, output_path: str) -> None:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        aesgcm = AESGCM(self._key)

        log.info("Decrypting with AES-256-GCM: %s -> %s", input_path, output_path)

        with open(input_path, "rb") as fin, open(output_path, "wb") as fout:
            magic = fin.read(len(MAGIC))
            if magic != MAGIC:
                raise RuntimeError(
                    "Invalid AES-256-GCM file: magic header mismatch"
                )

            base_nonce = fin.read(NONCE_SIZE)
            if len(base_nonce) != NONCE_SIZE:
                raise RuntimeError(
                    "Invalid AES-256-GCM file: truncated base nonce"
                )

            chunk_index = 0
            while True:
                length_bytes = fin.read(4)
                if not length_bytes:
                    break
                if len(length_bytes) < 4:
                    raise RuntimeError(
                        "Invalid AES-256-GCM file: truncated chunk length"
                    )

                segment_len = struct.unpack(">I", length_bytes)[0]
                segment = fin.read(segment_len)
                if len(segment) != segment_len:
                    raise RuntimeError(
                        "Invalid AES-256-GCM file: truncated chunk data"
                    )

                nonce = segment[:NONCE_SIZE]
                ciphertext = segment[NONCE_SIZE:]

                expected_nonce = _derive_nonce(base_nonce, chunk_index)
                if nonce != expected_nonce:
                    raise RuntimeError(
                        f"Invalid AES-256-GCM file: nonce mismatch at chunk {chunk_index}"
                    )

                plaintext = aesgcm.decrypt(nonce, ciphertext, None)
                fout.write(plaintext)

                chunk_index += 1

    def file_suffix(self) -> str:
        return ".enc"


def _derive_nonce(base_nonce: bytes, chunk_index: int) -> bytes:
    """Derive a unique nonce for each chunk by XORing base_nonce with the chunk index."""
    index_bytes = chunk_index.to_bytes(NONCE_SIZE, byteorder="big")
    return bytes(a ^ b for a, b in zip(base_nonce, index_bytes))


def create(config: dict) -> AES256GCMEncryptor:
    """Factory function for AES256GCMEncryptor."""
    key_hex = config.get("key")
    key_file = config.get("key_file")

    if key_hex:
        try:
            key = bytes.fromhex(key_hex)
        except ValueError as e:
            raise ConfigError(f"AES-256-GCM key is not valid hex: {e}") from e
    elif key_file:
        try:
            with open(key_file, "rb") as f:
                raw = f.read()
            # Try hex-encoded key first (strip whitespace only for text decoding)
            try:
                key = bytes.fromhex(raw.decode("ascii").strip())
            except (ValueError, UnicodeDecodeError):
                # Raw binary key — do NOT strip (could remove valid key bytes)
                key = raw
        except FileNotFoundError:
            raise ConfigError(f"AES-256-GCM key file not found: {key_file}")
    else:
        raise ConfigError(
            "AES-256-GCM encryptor requires either 'key_env'/'key' or 'key_file'"
        )

    return AES256GCMEncryptor(key=key)
