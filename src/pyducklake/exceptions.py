"""Ducklake exception hierarchy."""

from __future__ import annotations

__all__ = [
    "DucklakeError",
    "NoSuchTableError",
    "TableAlreadyExistsError",
    "NoSuchNamespaceError",
    "NamespaceAlreadyExistsError",
    "NamespaceNotEmptyError",
    "CommitFailedError",
    "NoSuchViewError",
    "ViewAlreadyExistsError",
]


class DucklakeError(Exception):
    """Base exception for all Ducklake errors."""


class NoSuchTableError(DucklakeError):
    """Raised when a table does not exist."""


class TableAlreadyExistsError(DucklakeError):
    """Raised when attempting to create a table that already exists."""


class NoSuchNamespaceError(DucklakeError):
    """Raised when a namespace does not exist."""


class NamespaceAlreadyExistsError(DucklakeError):
    """Raised when attempting to create a namespace that already exists."""


class NamespaceNotEmptyError(DucklakeError):
    """Raised when attempting to drop a non-empty namespace."""


class CommitFailedError(DucklakeError):
    """Raised when a commit operation fails."""


class NoSuchViewError(DucklakeError):
    """Raised when a view does not exist."""


class ViewAlreadyExistsError(DucklakeError):
    """Raised when attempting to create a view that already exists."""
