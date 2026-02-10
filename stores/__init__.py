"""Storage backend interface and factory."""

from __future__ import annotations

import importlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone


# All recognized backup file extensions, compound extensions first so
# they are matched before their shorter suffixes.
BACKUP_EXTENSIONS = [
    ".sql.gz", ".sql.zst", ".sql.lz4",
    ".dump.gz", ".dump.zst", ".dump.lz4",
    ".sql", ".dump",
]


def is_backup_file(filename: str) -> bool:
    """Return True if *filename* ends with a recognized backup extension."""
    return any(filename.endswith(ext) for ext in BACKUP_EXTENSIONS)


@dataclass
class BackupInfo:
    """Metadata for a single backup file in a store."""

    key: str  # full path/key in the store
    filename: str  # just the filename portion
    timestamp: datetime  # parsed from filename
    size: int  # bytes


def parse_timestamp(filename: str) -> datetime | None:
    """Parse YYYYMMDD-HHMMSS from a backup filename like 'mydb-20260210-143000.sql.gz'."""
    name = filename
    for ext in BACKUP_EXTENSIONS:
        if filename.endswith(ext):
            name = filename.removesuffix(ext)
            break
    parts = name.rsplit("-", 2)
    if len(parts) < 3:
        return None
    date_part, time_part = parts[-2], parts[-1]
    try:
        return datetime.strptime(f"{date_part}-{time_part}", "%Y%m%d-%H%M%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None


class Store(ABC):
    """Abstract base for backup storage backends."""

    @abstractmethod
    def upload(self, local_path: str, remote_key: str) -> None:
        """Upload a local file to the store."""

    @abstractmethod
    def download(self, remote_key: str, local_path: str) -> None:
        """Download a file from the store to a local path."""

    @abstractmethod
    def list(self, prefix: str) -> list[BackupInfo]:
        """List backup files under the given prefix, sorted oldest-first."""

    @abstractmethod
    def delete(self, remote_key: str) -> None:
        """Delete a file from the store."""


# Map of store type names to module names within this package.
_STORE_TYPES = {
    "s3": "s3",
    "ssh": "ssh",
}


def create_store(config: dict) -> Store:
    """Create a Store instance from a store config dict.

    The config must have a 'type' key (e.g. 's3', 'ssh').
    Remaining keys are passed to the store's constructor.
    """
    store_type = config.get("type")
    if store_type not in _STORE_TYPES:
        raise ValueError(
            f"Unknown store type '{store_type}'. "
            f"Available: {', '.join(_STORE_TYPES)}"
        )

    module = importlib.import_module(f".{_STORE_TYPES[store_type]}", package=__name__)
    return module.create(config)
