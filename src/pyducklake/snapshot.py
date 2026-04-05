"""Ducklake snapshot representation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

__all__ = ["Snapshot"]


@dataclass(frozen=True)
class Snapshot:
    """Represents a Ducklake snapshot (committed transaction)."""

    snapshot_id: int
    timestamp: datetime
    schema_version: int | None = None
    changes: str | None = None
    author: str | None = None
    commit_message: str | None = None
