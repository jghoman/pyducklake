"""Ducklake catalog backed by DuckDB + ducklake extension."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow as pa

if TYPE_CHECKING:
    from pyducklake.transaction import Transaction

from pyducklake.exceptions import (
    DucklakeError,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    NoSuchViewError,
    TableAlreadyExistsError,
    ViewAlreadyExistsError,
)
from pyducklake.schema import Schema
from pyducklake.table import Table
from pyducklake.types import (
    BigIntType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    DucklakeType,
    FloatType,
    HugeIntType,
    IntegerType,
    IntervalType,
    JSONType,
    ListType,
    MapType,
    NestedField,
    SmallIntType,
    StringType,
    StructType,
    TimestampType,
    TimestampTZType,
    TimeType,
    TinyIntType,
    UBigIntType,
    UIntegerType,
    USmallIntType,
    UTinyIntType,
    UUIDType,
    ducklake_type_to_sql,
)
from pyducklake.view import View

__all__ = ["Catalog", "escape_string_literal", "quote_identifier"]

# Mapping from DuckDB type strings to DucklakeType constructors
_DUCKDB_TYPE_MAP: dict[str, DucklakeType] = {
    "BOOLEAN": BooleanType(),
    "TINYINT": TinyIntType(),
    "SMALLINT": SmallIntType(),
    "INTEGER": IntegerType(),
    "INT": IntegerType(),
    "BIGINT": BigIntType(),
    "HUGEINT": HugeIntType(),
    "UTINYINT": UTinyIntType(),
    "USMALLINT": USmallIntType(),
    "UINTEGER": UIntegerType(),
    "UBIGINT": UBigIntType(),
    "FLOAT": FloatType(),
    "REAL": FloatType(),
    "DOUBLE": DoubleType(),
    "VARCHAR": StringType(),
    "TEXT": StringType(),
    "STRING": StringType(),
    "BLOB": BinaryType(),
    "BYTEA": BinaryType(),
    "DATE": DateType(),
    "TIME": TimeType(),
    "TIMESTAMP": TimestampType(),
    "DATETIME": TimestampType(),
    "TIMESTAMP WITH TIME ZONE": TimestampTZType(),
    "TIMESTAMPTZ": TimestampTZType(),
    "UUID": UUIDType(),
    "JSON": JSONType(),
    "INTERVAL": IntervalType(),
}

_DECIMAL_RE = re.compile(r"^DECIMAL\((\d+),\s*(\d+)\)$", re.IGNORECASE)
_STRUCT_RE = re.compile(r"^STRUCT\((.+)\)$", re.IGNORECASE)
_MAP_RE = re.compile(r"^MAP\((.+)\)$", re.IGNORECASE)
_LIST_RE = re.compile(r"^(.+)\[\]$")


def _duckdb_type_to_ducklake(type_str: str, field_id_counter: list[int] | None = None) -> DucklakeType:
    """Map DuckDB type strings to DucklakeType."""
    type_str = type_str.strip()

    # Direct lookup
    upper = type_str.upper()
    direct = _DUCKDB_TYPE_MAP.get(upper)
    if direct is not None:
        return direct

    # DECIMAL(p, s)
    m = _DECIMAL_RE.match(type_str)
    if m:
        return DecimalType(int(m.group(1)), int(m.group(2)))

    # type[] (LIST)
    m = _LIST_RE.match(type_str)
    if m:
        elem_type = _duckdb_type_to_ducklake(m.group(1), field_id_counter)
        eid = _next_id(field_id_counter)
        return ListType(element_id=eid, element_type=elem_type)

    # MAP(k, v)
    m = _MAP_RE.match(type_str)
    if m:
        inner = m.group(1)
        parts = _split_top_level(inner)
        if len(parts) != 2:
            raise ValueError(f"Cannot parse MAP type: {type_str}")
        key_type = _duckdb_type_to_ducklake(parts[0].strip(), field_id_counter)
        kid = _next_id(field_id_counter)
        value_type = _duckdb_type_to_ducklake(parts[1].strip(), field_id_counter)
        vid = _next_id(field_id_counter)
        return MapType(key_id=kid, key_type=key_type, value_id=vid, value_type=value_type)

    # STRUCT(...)
    m = _STRUCT_RE.match(type_str)
    if m:
        inner = m.group(1)
        field_defs = _split_top_level(inner)
        fields: list[NestedField] = []
        for fdef in field_defs:
            fdef = fdef.strip()
            # "name TYPE" — split on first whitespace
            space_idx = fdef.index(" ")
            fname = fdef[:space_idx].strip().strip('"')
            ftype_str = fdef[space_idx + 1 :].strip()
            ftype = _duckdb_type_to_ducklake(ftype_str, field_id_counter)
            fid = _next_id(field_id_counter)
            fields.append(NestedField(field_id=fid, name=fname, field_type=ftype))
        return StructType(fields=tuple(fields))

    raise ValueError(f"Cannot parse DuckDB type: {type_str}")


def _next_id(counter: list[int] | None) -> int:
    """Get next field ID from counter, or return 0 if no counter."""
    if counter is None:
        return 0
    val = counter[0]
    counter[0] += 1
    return val


def _split_top_level(s: str) -> list[str]:
    """Split a string by commas, respecting parentheses and bracket nesting."""
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in s:
        if ch in ("(", "["):
            depth += 1
            current.append(ch)
        elif ch in (")", "]"):
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(ch)
    if current:
        parts.append("".join(current))
    return parts


def quote_identifier(name: str) -> str:
    """Double-quote an identifier, escaping embedded double-quotes by doubling them."""
    return '"' + name.replace('"', '""') + '"'


def escape_string_literal(value: str) -> str:
    """Escape a string for use in a SQL string literal (single-quoted)."""
    return value.replace("'", "''")


class Catalog:
    """Ducklake catalog backed by DuckDB + ducklake extension."""

    def __init__(
        self,
        name: str,
        uri: str,
        *,
        data_path: str | None = None,
        properties: dict[str, str] | None = None,
        encrypted: bool = False,
    ) -> None:
        self._name = name
        self._uri = uri
        self._data_path = data_path
        self._encrypted = encrypted

        self._conn = duckdb.connect()
        self._conn.execute("INSTALL ducklake; LOAD ducklake;")

        # Apply connection properties (e.g., S3 configuration)
        for key, value in (properties or {}).items():
            if not self._OPTION_KEY_RE.match(key):
                raise ValueError(
                    f"Invalid property key: {key!r}. Keys must contain only alphanumeric characters and underscores."
                )
            self._conn.execute(f"SET {key} = '{escape_string_literal(value)}'")

        attach_sql = f"ATTACH 'ducklake:{uri}' AS {quote_identifier(name)}"
        options: list[str] = []
        if data_path is not None:
            options.append(f"DATA_PATH '{escape_string_literal(data_path)}'")
        if encrypted:
            options.append("ENCRYPTED")
        if options:
            attach_sql += f" ({', '.join(options)})"
        self._conn.execute(attach_sql)

    @property
    def name(self) -> str:
        return self._name

    @property
    def encrypted(self) -> bool:
        """Whether this catalog uses Parquet encryption."""
        return self._encrypted

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Access the underlying DuckDB connection."""
        return self._conn

    # -- Namespace operations ------------------------------------------------

    def list_namespaces(self) -> list[str]:
        """List all namespaces (schemas)."""
        rows = self.fetchall(
            "SELECT schema_name FROM information_schema.schemata "
            f"WHERE catalog_name = '{escape_string_literal(self._name)}' "
            "ORDER BY schema_name"
        )
        return [row[0] for row in rows]

    def create_namespace(self, namespace: str) -> None:
        """CREATE SCHEMA. Raises NamespaceAlreadyExistsError if exists."""
        if self.namespace_exists(namespace):
            raise NamespaceAlreadyExistsError(f"Namespace already exists: {namespace}")
        self._execute(f"CREATE SCHEMA {quote_identifier(self._name)}.{quote_identifier(namespace)}")

    def create_namespace_if_not_exists(self, namespace: str) -> None:
        """Create namespace, no-op if already exists."""
        self._execute(f"CREATE SCHEMA IF NOT EXISTS {quote_identifier(self._name)}.{quote_identifier(namespace)}")

    def drop_namespace(self, namespace: str) -> None:
        """DROP SCHEMA. Raises NamespaceNotEmptyError if non-empty, NoSuchNamespaceError if not found."""
        if not self.namespace_exists(namespace):
            raise NoSuchNamespaceError(f"Namespace does not exist: {namespace}")
        tables = self.list_tables(namespace)
        views = self.list_views(namespace)
        if tables or views:
            raise NamespaceNotEmptyError(f"Namespace is not empty: {namespace}")
        self._execute(f"DROP SCHEMA {quote_identifier(self._name)}.{quote_identifier(namespace)}")

    def namespace_exists(self, namespace: str) -> bool:
        rows = self.fetchall(
            "SELECT 1 FROM information_schema.schemata "
            f"WHERE catalog_name = '{escape_string_literal(self._name)}' "
            f"AND schema_name = '{escape_string_literal(namespace)}'"
        )
        return len(rows) > 0

    # -- Table operations ----------------------------------------------------

    def list_tables(self, namespace: str = "main") -> list[tuple[str, str]]:
        """List tables in namespace. Returns list of (namespace, table_name) tuples."""
        rows = self.fetchall(
            "SELECT table_schema, table_name FROM information_schema.tables "
            f"WHERE table_catalog = '{escape_string_literal(self._name)}' "
            f"AND table_schema = '{escape_string_literal(namespace)}' "
            "AND table_type != 'VIEW' "
            "ORDER BY table_name"
        )
        return [(row[0], row[1]) for row in rows]

    def create_table(
        self,
        identifier: str | tuple[str, str],
        schema: Schema,
    ) -> Table:
        """CREATE TABLE with the given schema. Raises TableAlreadyExistsError if exists."""
        namespace, table_name = self._resolve_identifier(identifier)

        if self.table_exists((namespace, table_name)):
            raise TableAlreadyExistsError(f"Table already exists: {namespace}.{table_name}")

        fqn = self.fully_qualified_name(namespace, table_name)
        col_defs = ", ".join(
            f"{quote_identifier(f.name)} {ducklake_type_to_sql(f.field_type)}{' NOT NULL' if f.required else ''}"
            for f in schema.fields
        )
        self._execute(f"CREATE TABLE {fqn} ({col_defs})")

        # Reload schema from DuckDB to get canonical types
        loaded_schema = self.build_schema_from_describe(namespace, table_name)
        return Table(identifier=(namespace, table_name), schema=loaded_schema, catalog=self)

    def create_table_if_not_exists(
        self,
        identifier: str | tuple[str, str],
        schema: Schema,
    ) -> Table:
        """Create table if it doesn't exist, otherwise load and return existing."""
        namespace, table_name = self._resolve_identifier(identifier)
        if self.table_exists((namespace, table_name)):
            return self.load_table((namespace, table_name))
        return self.create_table((namespace, table_name), schema)

    def load_table(self, identifier: str | tuple[str, str]) -> Table:
        """Load table metadata. Raises NoSuchTableError if not found."""
        namespace, table_name = self._resolve_identifier(identifier)
        if not self.table_exists((namespace, table_name)):
            raise NoSuchTableError(f"Table does not exist: {namespace}.{table_name}")
        schema = self.build_schema_from_describe(namespace, table_name)
        return Table(identifier=(namespace, table_name), schema=schema, catalog=self)

    def drop_table(self, identifier: str | tuple[str, str]) -> None:
        """DROP TABLE. Raises NoSuchTableError if not found."""
        namespace, table_name = self._resolve_identifier(identifier)
        if not self.table_exists((namespace, table_name)):
            raise NoSuchTableError(f"Table does not exist: {namespace}.{table_name}")
        fqn = self.fully_qualified_name(namespace, table_name)
        self._execute(f"DROP TABLE {fqn}")

    def rename_table(self, from_identifier: str | tuple[str, str], to_identifier: str | tuple[str, str]) -> Table:
        """ALTER TABLE RENAME TO. Returns the renamed table."""
        from_ns, from_name = self._resolve_identifier(from_identifier)
        to_ns, to_name = self._resolve_identifier(to_identifier)

        if not self.table_exists((from_ns, from_name)):
            raise NoSuchTableError(f"Table does not exist: {from_ns}.{from_name}")

        if from_ns != to_ns:
            raise DucklakeError("Cross-namespace rename is not supported")

        from_fqn = self.fully_qualified_name(from_ns, from_name)
        # DuckDB RENAME TO expects just the new table name, not fully qualified
        self._execute(f"ALTER TABLE {from_fqn} RENAME TO {quote_identifier(to_name)}")
        return self.load_table((to_ns, to_name))

    def table_exists(self, identifier: str | tuple[str, str]) -> bool:
        namespace, table_name = self._resolve_identifier(identifier)
        rows = self.fetchall(
            "SELECT 1 FROM information_schema.tables "
            f"WHERE table_catalog = '{escape_string_literal(self._name)}' "
            f"AND table_schema = '{escape_string_literal(namespace)}' "
            f"AND table_name = '{escape_string_literal(table_name)}'"
        )
        return len(rows) > 0

    # -- View operations -----------------------------------------------------

    def create_view(
        self,
        identifier: str | tuple[str, str],
        sql: str,
    ) -> View:
        """Create a view in the catalog.

        Uses: CREATE VIEW {fqn} AS {sql}

        Raises ViewAlreadyExistsError if the view already exists.
        """
        namespace, view_name = self._resolve_identifier(identifier)
        if self.view_exists((namespace, view_name)):
            raise ViewAlreadyExistsError(f"View already exists: {namespace}.{view_name}")
        fqn = self.fully_qualified_name(namespace, view_name)
        self._execute(f"CREATE VIEW {fqn} AS {sql}")
        return self.load_view((namespace, view_name))

    def create_or_replace_view(
        self,
        identifier: str | tuple[str, str],
        sql: str,
    ) -> View:
        """Create or replace a view. Returns the View object."""
        namespace, view_name = self._resolve_identifier(identifier)
        fqn = self.fully_qualified_name(namespace, view_name)
        self._execute(f"CREATE OR REPLACE VIEW {fqn} AS {sql}")
        return self.load_view((namespace, view_name))

    def load_view(self, identifier: str | tuple[str, str]) -> View:
        """Load a view. Raises NoSuchViewError if not found.

        Queries the view's schema and SQL definition.
        """
        namespace, view_name = self._resolve_identifier(identifier)
        if not self.view_exists((namespace, view_name)):
            raise NoSuchViewError(f"View does not exist: {namespace}.{view_name}")
        schema = self.build_schema_from_describe(namespace, view_name)
        # Get the SQL definition
        rows = self.fetchall(
            "SELECT view_definition FROM information_schema.views "
            f"WHERE table_catalog = '{escape_string_literal(self._name)}' "
            f"AND table_schema = '{escape_string_literal(namespace)}' "
            f"AND table_name = '{escape_string_literal(view_name)}'"
        )
        sql_text = rows[0][0] if rows else ""
        return View(identifier=(namespace, view_name), schema=schema, sql=sql_text, catalog=self)

    def rename_view(self, from_identifier: str | tuple[str, str], to_identifier: str | tuple[str, str]) -> View:
        """Rename a view. Raises NoSuchViewError if not found."""
        from_ns, from_name = self._resolve_identifier(from_identifier)
        to_ns, to_name = self._resolve_identifier(to_identifier)

        if not self.view_exists((from_ns, from_name)):
            raise NoSuchViewError(f"View does not exist: {from_ns}.{from_name}")

        if from_ns != to_ns:
            raise DucklakeError("Cross-namespace rename is not supported")

        from_fqn = self.fully_qualified_name(from_ns, from_name)
        self._execute(f"ALTER VIEW {from_fqn} RENAME TO {quote_identifier(to_name)}")
        return self.load_view((to_ns, to_name))

    def drop_view(self, identifier: str | tuple[str, str]) -> None:
        """Drop a view. Raises NoSuchViewError if not found."""
        namespace, view_name = self._resolve_identifier(identifier)
        if not self.view_exists((namespace, view_name)):
            raise NoSuchViewError(f"View does not exist: {namespace}.{view_name}")
        fqn = self.fully_qualified_name(namespace, view_name)
        self._execute(f"DROP VIEW {fqn}")

    def list_views(self, namespace: str = "main") -> list[tuple[str, str]]:
        """List views in namespace. Returns list of (namespace, view_name) tuples."""
        rows = self.fetchall(
            "SELECT table_schema, table_name FROM information_schema.tables "
            f"WHERE table_catalog = '{escape_string_literal(self._name)}' "
            f"AND table_schema = '{escape_string_literal(namespace)}' "
            "AND table_type = 'VIEW' "
            "ORDER BY table_name"
        )
        return [(row[0], row[1]) for row in rows]

    def view_exists(self, identifier: str | tuple[str, str]) -> bool:
        """Check if a view exists."""
        namespace, view_name = self._resolve_identifier(identifier)
        rows = self.fetchall(
            "SELECT 1 FROM information_schema.tables "
            f"WHERE table_catalog = '{escape_string_literal(self._name)}' "
            f"AND table_schema = '{escape_string_literal(namespace)}' "
            f"AND table_name = '{escape_string_literal(view_name)}' "
            "AND table_type = 'VIEW'"
        )
        return len(rows) > 0

    # -- Internal helpers ----------------------------------------------------

    def _resolve_identifier(self, identifier: str | tuple[str, str]) -> tuple[str, str]:
        """Parse identifier into (namespace, table_name). Default namespace is 'main'."""
        if isinstance(identifier, tuple):
            return identifier
        parts = identifier.split(".", 1)
        if len(parts) == 2:
            return (parts[0], parts[1])
        return ("main", parts[0])

    def fully_qualified_name(self, namespace: str, table_name: str) -> str:
        """Returns 'catalog_name.namespace.table_name' with proper quoting."""
        return f"{quote_identifier(self._name)}.{quote_identifier(namespace)}.{quote_identifier(table_name)}"

    def _execute(self, sql: str, params: list[Any] | None = None) -> duckdb.DuckDBPyConnection:
        """Execute SQL on the connection."""
        if params:
            return self._conn.execute(sql, params)
        return self._conn.execute(sql)

    def fetchall(self, sql: str) -> list[tuple[Any, ...]]:
        """Execute and fetchall."""
        result = self._conn.execute(sql)
        rows: list[tuple[Any, ...]] = result.fetchall()
        return rows

    def build_schema_from_describe(self, namespace: str, table_name: str) -> Schema:
        """Build a Schema from information_schema.columns for an existing table."""
        rows = self.fetchall(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            f"WHERE table_catalog = '{escape_string_literal(self._name)}' "
            f"AND table_schema = '{escape_string_literal(namespace)}' "
            f"AND table_name = '{escape_string_literal(table_name)}' "
            "ORDER BY ordinal_position"
        )
        # field_id_counter tracks IDs for nested types as well
        field_id_counter = [1]
        fields: list[NestedField] = []
        for col_name, col_type, is_nullable in rows:
            dtype = _duckdb_type_to_ducklake(col_type, field_id_counter)
            fid = field_id_counter[0]
            field_id_counter[0] += 1
            required = is_nullable == "NO"
            fields.append(NestedField(field_id=fid, name=col_name, field_type=dtype, required=required))
        return Schema(*fields)

    # -- Commit Metadata -----------------------------------------------------

    def set_commit_message(self, message: str, *, author: str | None = None) -> None:
        """Set commit message and optional author for the next transaction.

        Must be called inside an explicit ``BEGIN TRANSACTION`` / ``COMMIT``
        block for the metadata to be recorded on the snapshot.

        Uses: ``CALL {catalog}.set_commit_message(author, commit_message)``
        """
        cat = quote_identifier(self._name)
        author_sql = f"'{escape_string_literal(author)}'" if author is not None else "NULL"
        msg_sql = f"'{escape_string_literal(message)}'"
        self._execute(f"CALL {cat}.set_commit_message({author_sql}, {msg_sql})")

    # -- Configuration -------------------------------------------------------

    _OPTION_KEY_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
    _SCOPE_PART_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

    def set_option(
        self,
        key: str,
        value: str,
        *,
        scope: str | None = None,
    ) -> None:
        """Set a Ducklake configuration option.

        Args:
            key: Option key (e.g., ``'target_file_size'``).
            value: Option value.
            scope: Optional ``'schema.table'`` for table-level scope.
                   ``None`` for global scope.

        Uses: ``CALL {catalog}.set_option(option, value, table_name, schema)``

        Raises:
            ValueError: If ``key`` or ``scope`` contain invalid characters.
        """
        if not self._OPTION_KEY_RE.match(key):
            raise ValueError(
                f"Invalid option key: {key!r}. Keys must contain only alphanumeric characters and underscores."
            )

        cat = quote_identifier(self._name)
        key_sql = f"'{escape_string_literal(key)}'"
        value_sql = f"'{escape_string_literal(value)}'"
        if scope is not None:
            parts = scope.split(".", 1)
            if len(parts) == 2:
                schema_name, table_name = parts
            else:
                schema_name = "main"
                table_name = parts[0]
            for part_label, part_value in [("schema", schema_name), ("table", table_name)]:
                if not self._SCOPE_PART_RE.match(part_value):
                    raise ValueError(
                        f"Invalid scope {part_label}: {part_value!r}. "
                        "Scope parts must contain only alphanumeric characters and underscores."
                    )
            self._execute(
                f"CALL {cat}.set_option("
                f"{key_sql}, {value_sql}, "
                f"table_name := '{escape_string_literal(table_name)}', "
                f"schema := '{escape_string_literal(schema_name)}')"
            )
        else:
            self._execute(f"CALL {cat}.set_option({key_sql}, {value_sql})")

    def get_options(self) -> pa.Table:
        """Get all configuration options as an Arrow table.

        Columns: option_name, description, value, scope, scope_entry
        """
        cat = quote_identifier(self._name)
        result: Any = self._conn.execute(f"SELECT * FROM {cat}.options()")
        arrow_obj: Any = result.arrow()
        if isinstance(arrow_obj, pa.Table):
            return arrow_obj
        tbl: pa.Table = arrow_obj.read_all()
        return tbl

    # -- Transaction ---------------------------------------------------------

    def begin_transaction(self) -> Transaction:
        """Begin a multi-operation transaction.

        All table operations within the transaction are committed atomically.
        """
        from pyducklake.transaction import Transaction

        return Transaction(self)

    # -- Context manager -----------------------------------------------------

    def __enter__(self) -> Catalog:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()
