"""ChangeSet: result wrapper for CDC queries."""

# pyright: reportUnknownMemberType=false
# pyright: reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false

from __future__ import annotations

import importlib.util
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

__all__ = ["ChangeSet"]


class ChangeSet:
    """Result of a CDC query with helpers for analyzing changes."""

    def __init__(
        self,
        arrow_table: pa.Table,
        change_type_col: str | None = "change_type",
    ) -> None:
        self._table = arrow_table
        self._change_type_col = change_type_col

    # -- Accessors -------------------------------------------------------------

    def to_arrow(self) -> pa.Table:
        """Return the raw Arrow table."""
        return self._table

    def to_pandas(self) -> Any:
        """Return as pandas DataFrame.

        Requires pandas to be installed separately.
        """
        if importlib.util.find_spec("pandas") is None:
            raise ImportError("pandas is required for to_pandas(). Install it with: pip install pandas")
        return self._table.to_pandas()

    @property
    def num_rows(self) -> int:
        """Number of rows in the result."""
        result: int = self._table.num_rows
        return result

    @property
    def column_names(self) -> list[str]:
        """Column names in the result."""
        result: list[str] = self._table.column_names
        return result

    # -- Change type filtering -------------------------------------------------

    def _require_change_type(self) -> str:
        if self._change_type_col is None:
            raise ValueError(
                "This ChangeSet has no change_type column. "
                "Change type filtering is only available on table_changes() results."
            )
        return self._change_type_col

    def _filter_by_type(self, change_type: str) -> pa.Table:
        col = self._require_change_type()
        mask = pc.equal(self._table.column(col), pa.scalar(change_type))
        return self._table.filter(mask)

    def inserts(self) -> pa.Table:
        """Return only inserted rows (change_type = 'insert')."""
        return self._filter_by_type("insert")

    def deletes(self) -> pa.Table:
        """Return only deleted rows (change_type = 'delete')."""
        return self._filter_by_type("delete")

    def update_preimages(self) -> pa.Table:
        """Return pre-update row images (change_type = 'update_preimage')."""
        return self._filter_by_type("update_preimage")

    def update_postimages(self) -> pa.Table:
        """Return post-update row images (change_type = 'update_postimage')."""
        return self._filter_by_type("update_postimage")

    def updates(self) -> list[tuple[dict[str, Any], dict[str, Any]]]:
        """Return paired (old_row, new_row) for each update.

        Correlates update_preimage and update_postimage rows by rowid.
        Returns list of (preimage_dict, postimage_dict) tuples.
        """
        pre = self.update_preimages()
        post = self.update_postimages()

        pre_by_rowid: dict[Any, dict[str, Any]] = {}
        for row in pre.to_pylist():
            pre_by_rowid[row.get("rowid")] = row

        pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for row in post.to_pylist():
            rid = row.get("rowid")
            if rid in pre_by_rowid:
                pairs.append((pre_by_rowid[rid], row))

        return pairs

    def has_updates(self) -> bool:
        """Whether there are any update changes."""
        col = self._require_change_type()
        types = self._table.column(col).to_pylist()
        return "update_preimage" in types or "update_postimage" in types

    def summary(self) -> dict[str, int]:
        """Return count of each change type.

        Example: {"insert": 5, "delete": 2, "update_preimage": 1, "update_postimage": 1}
        """
        col = self._require_change_type()
        counts: dict[str, int] = {}
        for ct in self._table.column(col).to_pylist():
            key = str(ct)
            counts[key] = counts.get(key, 0) + 1
        return counts

    def __repr__(self) -> str:
        return f"ChangeSet(num_rows={self.num_rows}, columns={self.column_names})"
