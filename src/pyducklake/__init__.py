"""
# pyducklake

A Python SDK for [Ducklake](https://ducklake.select), providing a
[pyiceberg](https://py.iceberg.apache.org/)-like API for the Ducklake
lakehouse format.

## Quick Start

```python
from pyducklake import Catalog, Schema, required, optional, IntegerType, StringType

# Connect to a Ducklake catalog (DuckDB, PostgreSQL, MySQL, or SQLite metadata)
catalog = Catalog("my_lake", "metadata.duckdb", data_path="./data")

# Ergonomic schema creation
schema = Schema.of(
    required("id", IntegerType()),
    optional("name", StringType()),
)

# Or with a dict (all fields optional):
schema = Schema.of({"id": IntegerType(), "name": StringType()})

# Explicit field IDs (used internally when loading from catalog):
from pyducklake import NestedField
schema = Schema(
    NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
    NestedField(field_id=2, name="name", field_type=StringType()),
)

table = catalog.create_table("users", schema)

# Write data
import pyarrow as pa
df = pa.table({"id": [1, 2, 3], "name": ["alice", "bob", "carol"]})
table.append(df)

# Read data
result = table.scan().to_arrow()
print(result)
```

## Metadata Backends

Connect to any supported metadata backend by changing the URI:

| Backend | URI |
|---------|-----|
| DuckDB | `Catalog("lake", "meta.duckdb")` |
| PostgreSQL | `Catalog("lake", "postgres:dbname=mydb host=localhost")` |
| MySQL | `Catalog("lake", "mysql:host=localhost database=mydb")` |
| SQLite | `Catalog("lake", "sqlite:meta.sqlite")` |

## Key Features

- **Catalog management** — `Catalog` provides namespace and table CRUD,
  views, configuration, and commit metadata.

- **Table operations** — `Table` supports `append()`, `overwrite()`, `delete()`,
  `upsert()`, and `add_files()` for data mutation.

- **Scan API** — `DataScan` offers an immutable builder for filtered, projected,
  time-traveling reads with output to Arrow, pandas, Polars, DuckDB, or
  streaming `RecordBatchReader`.

- **Schema evolution** — `UpdateSchema` provides `add_column()`, `drop_column()`,
  `rename_column()`, and `update_column()` with a builder/context-manager pattern.

- **Partitioning** — `UpdateSpec` manages hidden partitioning with identity,
  year, month, day, and hour transforms.

- **Sort orders** — `UpdateSortOrder` configures sort orders applied during
  compaction.

- **Transactions** — `Transaction` groups multiple operations across tables
  into a single atomic commit.

- **Time travel** — Scan at any snapshot version or timestamp via
  `scan().with_snapshot(id)` or `scan().with_timestamp(ts)`.

- **Change data capture** — `table_changes()`, `table_insertions()`, and
  `table_deletions()` query row-level changes between snapshots.

- **Inspect API** — `InspectTable` exposes snapshot history, data files,
  and partition metadata as Arrow tables.

- **Maintenance** — `MaintenanceTable` provides `compact()`,
  `expire_snapshots()`, `rewrite_data_files()`, `cleanup_files()`, and
  `checkpoint()`.

- **CLI** — The `pyducklake` command-line tool provides catalog inspection
  and maintenance from the terminal.

## Modules

| Module | Description |
|--------|-------------|
| `pyducklake.catalog` | Catalog connection and namespace/table/view management |
| `pyducklake.table` | Table class with read, write, and metadata operations |
| `pyducklake.scan` | DataScan builder for filtered, projected reads |
| `pyducklake.schema` | Schema representation and field lookup |
| `pyducklake.schema_evolution` | UpdateSchema builder for schema changes |
| `pyducklake.types` | Ducklake type system and Arrow/SQL conversion |
| `pyducklake.expressions` | Boolean expression tree for filters |
| `pyducklake.partitioning` | Partition transforms, specs, and UpdateSpec builder |
| `pyducklake.sorting` | Sort orders and UpdateSortOrder builder |
| `pyducklake.transaction` | Multi-operation atomic transactions |
| `pyducklake.inspect` | Metadata introspection (snapshots, files, partitions) |
| `pyducklake.maintenance` | Table maintenance (compaction, expiration, cleanup) |
| `pyducklake.snapshot` | Snapshot dataclass |
| `pyducklake.exceptions` | Exception hierarchy |
| `pyducklake.cli` | Command-line interface |
"""

__version__ = "0.1.0"

from pyducklake.catalog import Catalog
from pyducklake.cdc import ChangeSet
from pyducklake.exceptions import (
    CommitFailedError,
    DucklakeError,
    NamespaceAlreadyExistsError,
    NamespaceNotEmptyError,
    NoSuchNamespaceError,
    NoSuchTableError,
    NoSuchViewError,
    TableAlreadyExistsError,
    ViewAlreadyExistsError,
)
from pyducklake.expressions import (
    AlwaysFalse,
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNaN,
    NotNull,
    Or,
    Reference,
)
from pyducklake.inspect import InspectTable
from pyducklake.maintenance import MaintenanceTable
from pyducklake.partitioning import (
    DAY,
    HOUR,
    IDENTITY,
    MONTH,
    UNPARTITIONED,
    YEAR,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    PartitionField,
    PartitionSpec,
    Transform,
    UpdateSpec,
    YearTransform,
)
from pyducklake.scan import DataScan
from pyducklake.schema import Schema, optional, required
from pyducklake.schema_evolution import UpdateSchema
from pyducklake.snapshot import Snapshot
from pyducklake.sorting import (
    UNSORTED,
    NullOrder,
    SortDirection,
    SortField,
    SortOrder,
    UpdateSortOrder,
)
from pyducklake.table import ArrowCompatible, ArrowStreamExportable, Table, UpsertResult
from pyducklake.transaction import Transaction
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
    arrow_type_to_ducklake,
    ducklake_type_to_arrow,
    ducklake_type_to_sql,
)
from pyducklake.view import View

__all__ = [
    "ArrowCompatible",
    "ArrowStreamExportable",
    "AlwaysFalse",
    "AlwaysTrue",
    "And",
    "BooleanExpression",
    "Catalog",
    "ChangeSet",
    "CommitFailedError",
    "DataScan",
    "DucklakeError",
    "EqualTo",
    "GreaterThan",
    "GreaterThanOrEqual",
    "In",
    "InspectTable",
    "MaintenanceTable",
    "IsNaN",
    "IsNull",
    "LessThan",
    "LessThanOrEqual",
    "NamespaceAlreadyExistsError",
    "NamespaceNotEmptyError",
    "NoSuchNamespaceError",
    "NoSuchTableError",
    "NoSuchViewError",
    "Not",
    "NotEqualTo",
    "NotIn",
    "NotNaN",
    "NotNull",
    "Or",
    "Reference",
    "Schema",
    "optional",
    "required",
    "Snapshot",
    "Table",
    "TableAlreadyExistsError",
    "View",
    "ViewAlreadyExistsError",
    "Transaction",
    "UpdateSchema",
    "UpdateSortOrder",
    "UpdateSpec",
    "UNSORTED",
    "UpsertResult",
    "NullOrder",
    "SortDirection",
    "SortField",
    "SortOrder",
    "DAY",
    "DayTransform",
    "HOUR",
    "HourTransform",
    "IDENTITY",
    "IdentityTransform",
    "MONTH",
    "MonthTransform",
    "PartitionField",
    "PartitionSpec",
    "Transform",
    "UNPARTITIONED",
    "YEAR",
    "YearTransform",
    "BigIntType",
    "BinaryType",
    "BooleanType",
    "DateType",
    "DecimalType",
    "DoubleType",
    "DucklakeType",
    "FloatType",
    "HugeIntType",
    "IntegerType",
    "IntervalType",
    "JSONType",
    "ListType",
    "MapType",
    "NestedField",
    "SmallIntType",
    "StringType",
    "StructType",
    "TimeType",
    "TimestampTZType",
    "TimestampType",
    "TinyIntType",
    "UBigIntType",
    "UIntegerType",
    "USmallIntType",
    "UTinyIntType",
    "UUIDType",
    "arrow_type_to_ducklake",
    "ducklake_type_to_arrow",
    "ducklake_type_to_sql",
]
