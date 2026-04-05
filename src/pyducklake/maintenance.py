"""Table maintenance operations for Ducklake."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyducklake.table import Table

__all__ = ["MaintenanceTable"]

_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d+)?)?$")


def validate_older_than(value: str) -> None:
    """Validate that older_than looks like a timestamp string."""
    if not _TIMESTAMP_RE.match(value):
        raise ValueError(f"Invalid older_than value: {value!r}. Expected format 'YYYY-MM-DD HH:MM:SS'.")


class MaintenanceTable:
    """Table maintenance operations. Obtained via ``table.maintenance()``."""

    def __init__(self, table: Table) -> None:
        self._table = table

    def compact(
        self,
        *,
        min_file_size: int | None = None,
        max_file_size: int | None = None,
        max_compacted_files: int | None = None,
    ) -> None:
        """Merge small files into larger ones.

        Operates at the catalog level (ducklake does not support per-table compaction).
        """
        from pyducklake.catalog import escape_string_literal

        catalog_name = self._table.catalog.name
        conn = self._table.catalog.connection

        params: list[str] = [f"'{escape_string_literal(catalog_name)}'"]
        if min_file_size is not None:
            params.append(f"min_file_size := {min_file_size}::UBIGINT")
        if max_file_size is not None:
            params.append(f"max_file_size := {max_file_size}::UBIGINT")
        if max_compacted_files is not None:
            params.append(f"max_compacted_files := {max_compacted_files}::UBIGINT")

        sql = f"CALL ducklake_merge_adjacent_files({', '.join(params)})"
        conn.execute(sql)

    def rewrite_data_files(
        self,
        *,
        delete_threshold: float | None = None,
    ) -> None:
        """Rewrite files with excessive deletions.

        Operates at the catalog level.
        """
        from pyducklake.catalog import escape_string_literal

        catalog_name = self._table.catalog.name
        conn = self._table.catalog.connection

        params: list[str] = [f"'{escape_string_literal(catalog_name)}'"]
        if delete_threshold is not None:
            params.append(f"delete_threshold := {delete_threshold}")

        sql = f"CALL ducklake_rewrite_data_files({', '.join(params)})"
        conn.execute(sql)

    def expire_snapshots(
        self,
        *,
        older_than: str | None = None,
        versions: int | None = None,
        dry_run: bool = False,
    ) -> None:
        """Remove old snapshots.

        Args:
            older_than: Timestamp string (``'YYYY-MM-DD HH:MM:SS'``).
            versions: Number of versions to keep.
            dry_run: If True, report what would be expired without acting.
        """
        from pyducklake.catalog import escape_string_literal

        catalog_name = self._table.catalog.name
        conn = self._table.catalog.connection

        params: list[str] = [f"'{escape_string_literal(catalog_name)}'"]
        if older_than is not None:
            validate_older_than(older_than)
            params.append(f"older_than := '{escape_string_literal(older_than)}'")
        if versions is not None:
            params.append(f"versions := [{versions}::UBIGINT]")
        if dry_run:
            params.append("dry_run := true")

        sql = f"CALL ducklake_expire_snapshots({', '.join(params)})"
        conn.execute(sql)

    def cleanup_files(
        self,
        *,
        older_than: str | None = None,
        dry_run: bool = False,
    ) -> None:
        """Delete files scheduled for removal.

        Args:
            older_than: Timestamp string (``'YYYY-MM-DD HH:MM:SS'``).
            dry_run: If True, report what would be cleaned without acting.
        """
        from pyducklake.catalog import escape_string_literal

        catalog_name = self._table.catalog.name
        conn = self._table.catalog.connection

        params: list[str] = [f"'{escape_string_literal(catalog_name)}'"]
        if older_than is not None:
            validate_older_than(older_than)
            params.append(f"older_than := '{escape_string_literal(older_than)}'")
        if dry_run:
            params.append("dry_run := true")

        sql = f"CALL ducklake_cleanup_old_files({', '.join(params)})"
        conn.execute(sql)

    def delete_orphaned_files(self, *, dry_run: bool = False) -> None:
        """Remove untracked files from storage."""
        from pyducklake.catalog import escape_string_literal

        catalog_name = self._table.catalog.name
        conn = self._table.catalog.connection

        params: list[str] = [f"'{escape_string_literal(catalog_name)}'"]
        if dry_run:
            params.append("dry_run := true")

        sql = f"CALL ducklake_delete_orphaned_files({', '.join(params)})"
        conn.execute(sql)

    def checkpoint(self) -> None:
        """Run all maintenance operations sequentially."""
        catalog_name = self._table.catalog.name
        conn = self._table.catalog.connection
        conn.execute(f'CHECKPOINT "{catalog_name}"')
