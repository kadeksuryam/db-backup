"""Encryption backend interface and factory."""

from __future__ import annotations

import importlib
from abc import ABC, abstractmethod

from config import ConfigError


class Encryptor(ABC):
    """Abstract base for encryption backends."""

    @abstractmethod
    def encrypt(self, input_path: str, output_path: str) -> None:
        """Encrypt a file from input_path to output_path."""

    @abstractmethod
    def decrypt(self, input_path: str, output_path: str) -> None:
        """Decrypt a file from input_path to output_path."""

    @abstractmethod
    def file_suffix(self) -> str:
        """Return the file suffix for encrypted files (e.g. '.age')."""


# Map of encryptor type names to module names within this package.
_ENCRYPTOR_TYPES = {
    "age": "age",
    "gpg": "gpg",
    "aes-256-gcm": "aes256gcm",
}


def create_encryptor(config: dict) -> Encryptor:
    """Create an Encryptor instance from an encryption config dict.

    The config must have a 'type' key (e.g. 'age', 'gpg', 'aes-256-gcm').
    Remaining keys are passed to the encryptor's constructor.
    """
    enc_type = config.get("type")
    if enc_type not in _ENCRYPTOR_TYPES:
        raise ConfigError(
            f"Unknown encryptor type '{enc_type}'. "
            f"Available: {', '.join(_ENCRYPTOR_TYPES)}"
        )

    module = importlib.import_module(f".{_ENCRYPTOR_TYPES[enc_type]}", package=__name__)
    return module.create(config)
