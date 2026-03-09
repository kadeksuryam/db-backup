"""Age encryption backend."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess

from config import ConfigError
from encryptors import Encryptor

log = logging.getLogger(__name__)


class AgeEncryptor(Encryptor):
    """Encrypt/decrypt using the age tool.

    Supports two modes:
    - Recipient mode: encrypt with public key(s), decrypt with identity file
    - Passphrase mode: symmetric encryption using AGE_PASSPHRASE env var
    """

    def __init__(
        self,
        recipients: list[str] | None = None,
        identity: str | None = None,
        passphrase_env: str | None = None,
        age_binary: str = "age",
    ):
        self._recipients = recipients or []
        self._identity = identity
        self._passphrase_env = passphrase_env
        self._age_binary = age_binary

        binary = shutil.which(self._age_binary)
        if not binary:
            raise ConfigError(
                f"'{self._age_binary}' binary not found in PATH. "
                f"Install age: https://github.com/FiloSottile/age"
            )
        self._age_binary = binary

        if not self._recipients and not self._passphrase_env:
            raise ConfigError(
                "Age encryptor requires either 'recipients'/'recipient' or 'passphrase_env'"
            )

    def encrypt(self, input_path: str, output_path: str) -> None:
        cmd = [self._age_binary, "--encrypt"]
        env = _minimal_env()

        if self._recipients:
            for r in self._recipients:
                cmd.extend(["--recipient", r])
        elif self._passphrase_env:
            passphrase = os.environ.get(self._passphrase_env)
            if passphrase is None:
                raise RuntimeError(
                    f"Environment variable '{self._passphrase_env}' is not set"
                )
            env["AGE_PASSPHRASE"] = passphrase
            cmd.append("--passphrase")

        cmd.extend(["-o", output_path, input_path])
        log.info("Encrypting with age: %s -> %s", input_path, output_path)
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if result.returncode != 0:
            raise RuntimeError(f"age encrypt failed: {result.stderr.strip()}")

    def decrypt(self, input_path: str, output_path: str) -> None:
        cmd = [self._age_binary, "--decrypt"]
        env = _minimal_env()

        if self._identity:
            cmd.extend(["--identity", self._identity])
        elif self._passphrase_env:
            passphrase = os.environ.get(self._passphrase_env)
            if passphrase is None:
                raise RuntimeError(
                    f"Environment variable '{self._passphrase_env}' is not set"
                )
            env["AGE_PASSPHRASE"] = passphrase

        cmd.extend(["-o", output_path, input_path])
        log.info("Decrypting with age: %s -> %s", input_path, output_path)
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if result.returncode != 0:
            raise RuntimeError(f"age decrypt failed: {result.stderr.strip()}")

    def file_suffix(self) -> str:
        return ".age"


def _minimal_env() -> dict:
    """Build a minimal environment for subprocess calls."""
    env = {}
    for key in ("PATH", "HOME", "USER", "LANG", "LC_ALL"):
        if key in os.environ:
            env[key] = os.environ[key]
    return env


def create(config: dict) -> AgeEncryptor:
    """Factory function for AgeEncryptor."""
    recipients = config.get("recipients", [])
    if isinstance(recipients, str):
        recipients = [recipients]

    # Support singular 'recipient' key
    single = config.get("recipient")
    if single and single not in recipients:
        recipients.append(single)

    return AgeEncryptor(
        recipients=recipients or None,
        identity=config.get("identity"),
        passphrase_env=config.get("passphrase_env"),
        age_binary=config.get("age_binary", "age"),
    )
