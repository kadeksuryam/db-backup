"""GPG encryption backend."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess

from config import ConfigError
from encryptors import Encryptor

log = logging.getLogger(__name__)


class GPGEncryptor(Encryptor):
    """Encrypt/decrypt using GPG.

    Supports two modes:
    - Recipient mode: asymmetric encryption with a key ID
    - Symmetric mode: passphrase-based (AES256) via passphrase_env
    """

    def __init__(
        self,
        key_id: str | None = None,
        passphrase_env: str | None = None,
        gpg_binary: str = "gpg",
    ):
        self._key_id = key_id
        self._passphrase_env = passphrase_env
        self._gpg_binary = gpg_binary

        binary = shutil.which(self._gpg_binary)
        if not binary:
            raise ConfigError(
                f"'{self._gpg_binary}' binary not found in PATH. Install GnuPG."
            )
        self._gpg_binary = binary

        if not self._key_id and not self._passphrase_env:
            raise ConfigError(
                "GPG encryptor requires either 'key_id' or 'passphrase_env'"
            )

    def encrypt(self, input_path: str, output_path: str) -> None:
        if self._key_id:
            cmd = [
                self._gpg_binary, "--encrypt",
                "--recipient", self._key_id,
                "--batch", "--yes",
                "-o", output_path, input_path,
            ]
            log.info("Encrypting with GPG (recipient %s): %s -> %s",
                     self._key_id, input_path, output_path)
            result = subprocess.run(cmd, capture_output=True, text=True)
        else:
            passphrase = os.environ.get(self._passphrase_env)
            if passphrase is None:
                raise RuntimeError(
                    f"Environment variable '{self._passphrase_env}' is not set"
                )
            cmd = [
                self._gpg_binary, "--symmetric",
                "--cipher-algo", "AES256",
                "--batch", "--passphrase-fd", "0",
                "-o", output_path, input_path,
            ]
            log.info("Encrypting with GPG (symmetric): %s -> %s", input_path, output_path)
            result = subprocess.run(
                cmd, input=passphrase, capture_output=True, text=True,
            )

        if result.returncode != 0:
            raise RuntimeError(f"gpg encrypt failed: {result.stderr.strip()}")

    def decrypt(self, input_path: str, output_path: str) -> None:
        cmd = [
            self._gpg_binary, "--decrypt",
            "--batch",
        ]

        passphrase = None
        if self._passphrase_env:
            passphrase = os.environ.get(self._passphrase_env)
            if passphrase is None:
                raise RuntimeError(
                    f"Environment variable '{self._passphrase_env}' is not set"
                )
            cmd.extend(["--passphrase-fd", "0"])

        cmd.extend(["-o", output_path, input_path])
        log.info("Decrypting with GPG: %s -> %s", input_path, output_path)
        result = subprocess.run(
            cmd, input=passphrase, capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"gpg decrypt failed: {result.stderr.strip()}")

    def file_suffix(self) -> str:
        return ".gpg"


def create(config: dict) -> GPGEncryptor:
    """Factory function for GPGEncryptor."""
    return GPGEncryptor(
        key_id=config.get("key_id"),
        passphrase_env=config.get("passphrase_env"),
        gpg_binary=config.get("gpg_binary", "gpg"),
    )
