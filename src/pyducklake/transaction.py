"""Multi-operation transaction support for Ducklake catalogs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyducklake.exceptions import DucklakeError

if TYPE_CHECKING:
    from pyducklake.catalog import Catalog
    from pyducklake.table import Table

__all__ = ["Transaction"]


class Transaction:
    """Multi-operation transaction on a Ducklake catalog.

    Groups multiple write operations (append, overwrite, delete, schema changes)
    into a single atomic commit = single snapshot.

    Usage:
        with catalog.begin_transaction() as txn:
            tbl = txn.load_table("my_table")
            tbl.append(df1)
            tbl.delete(EqualTo("status", "old"))

            tbl2 = txn.load_table("other_table")
            tbl2.append(df2)
        # auto-commits on clean exit, rolls back on exception

    Or without context manager:
        txn = catalog.begin_transaction()
        # ... operations ...
        txn.commit()  # or txn.rollback()
    """

    def __init__(self, catalog: Catalog) -> None:
        """Begin a transaction on the catalog's connection."""
        self._catalog = catalog
        self._committed = False
        self._rolled_back = False
        self._catalog.connection.execute("BEGIN TRANSACTION")

    def load_table(self, identifier: str | tuple[str, str]) -> Table:
        """Load a table within this transaction context."""
        return self._catalog.load_table(identifier)

    def commit(self) -> None:
        """Commit the transaction."""
        if self._committed or self._rolled_back:
            raise DucklakeError("Transaction already finalized")
        self._catalog.connection.execute("COMMIT")
        self._committed = True

    def rollback(self) -> None:
        """Roll back the transaction."""
        if self._committed or self._rolled_back:
            raise DucklakeError("Transaction already finalized")
        self._catalog.connection.execute("ROLLBACK")
        self._rolled_back = True

    @property
    def is_active(self) -> bool:
        """True if the transaction has not been committed or rolled back."""
        return not self._committed and not self._rolled_back

    def __enter__(self) -> Transaction:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if self._committed or self._rolled_back:
            return
        if exc_type is not None:
            self.rollback()
        else:
            self.commit()
