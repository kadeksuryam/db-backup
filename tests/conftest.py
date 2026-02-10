"""Shared fixtures for dbbackup tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Add the dbbackup package root to sys.path so imports work like they do at runtime.
_pkg_root = str(Path(__file__).resolve().parent.parent)
if _pkg_root not in sys.path:
    sys.path.insert(0, _pkg_root)
