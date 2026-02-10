"""Database engine interface and factory."""

from __future__ import annotations

import importlib
from abc import ABC, abstractmethod

from config import Datasource


class Engine(ABC):
    """Abstract base for database engine backends."""

    @abstractmethod
    def check_connectivity(self, ds: Datasource) -> None:
        """Verify the database is reachable."""

    @abstractmethod
    def check_version_compat(self, ds: Datasource) -> None:
        """Warn if client tools are incompatible with the server version."""

    @abstractmethod
    def dump(self, ds: Datasource, output_path: str) -> None:
        """Create a compressed backup file at output_path."""

    @abstractmethod
    def restore(self, ds: Datasource, input_path: str) -> None:
        """Restore the database from the backup file at input_path."""

    @abstractmethod
    def count_tables(self, ds: Datasource) -> int:
        """Count user tables in the database (for pre-restore safety check)."""

    @abstractmethod
    def drop_and_recreate(self, ds: Datasource) -> None:
        """Drop and recreate the target database."""

    @abstractmethod
    def file_extension(self) -> str:
        """Return the backup file extension (e.g. '.sql.gz')."""


# Map of engine type names to module names within this package.
_ENGINE_TYPES = {
    "postgres": "postgres",
}


def create_engine(engine_type: str) -> Engine:
    """Create an Engine instance by type name.

    The engine_type must match a key in _ENGINE_TYPES (e.g. 'postgres').
    """
    if engine_type not in _ENGINE_TYPES:
        raise ValueError(
            f"Unknown engine type '{engine_type}'. "
            f"Available: {', '.join(_ENGINE_TYPES)}"
        )

    module = importlib.import_module(f".{_ENGINE_TYPES[engine_type]}", package=__name__)
    return module.create()
