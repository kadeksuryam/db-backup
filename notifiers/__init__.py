"""Notification backend interface and factory."""

from __future__ import annotations

import importlib
from abc import ABC, abstractmethod

from config import ConfigError


class Notifier(ABC):
    """Abstract base for notification backends."""

    @abstractmethod
    def send(self, job_name: str, status: str, message: str) -> None:
        """Send a notification. status is 'success' or 'failure'."""


# Map of notifier type names to module names within this package.
_NOTIFIER_TYPES = {
    "email": "email",
}


def create_notifier(config: dict) -> Notifier:
    """Create a Notifier instance from a notifier config dict.

    The config must have a 'type' key (e.g. 'email').
    Remaining keys are passed to the notifier's constructor.
    """
    notifier_type = config.get("type")
    if notifier_type not in _NOTIFIER_TYPES:
        raise ConfigError(
            f"Unknown notifier type '{notifier_type}'. "
            f"Available: {', '.join(_NOTIFIER_TYPES)}"
        )

    module = importlib.import_module(f".{_NOTIFIER_TYPES[notifier_type]}", package=__name__)
    return module.create(config)
