"""Schema evolution: add, rename, drop columns and widen types.

Demonstrates all schema evolution operations while showing that existing
data is preserved through each change.
"""

import tempfile
from pathlib import Path

import pyarrow as pa

from pyducklake import (
    Catalog,
    Schema,
    required,
    optional,
    IntegerType,
    BigIntType,
    StringType,
    DoubleType,
)


def print_table(table, label: str) -> None:
    """Print the current schema and data for a table."""
    print(f"\n--- {label} ---")
    print(f"Schema: {table.schema.column_names()}")
    result = table.scan().to_arrow()
    if result.num_rows > 0:
        print(result.to_pandas().to_string(index=False))
    else:
        print("(empty)")


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        meta_path = str(Path(tmpdir) / "meta.duckdb")
        data_path = str(Path(tmpdir) / "data")

        catalog = Catalog("lake", meta_path, data_path=data_path)

        # --- 1. Create table with initial schema ---
        print("=== Initial schema ===")
        schema = Schema.of(
            required("id", IntegerType()),
            optional("name", StringType()),
            optional("score", IntegerType()),
        )
        table = catalog.create_table("players", schema)

        data = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "score": [100, 250, 175],
        })
        table.append(data)
        print_table(table, "Initial state")

        # --- 2. Add a column ---
        print("\n=== Add column: 'email' ===")
        with table.update_schema() as update:
            update.add_column("email", StringType())

        # Existing rows will have NULL for the new column
        print_table(table, "After adding 'email'")

        # Write data with the new column
        new_data = pa.table({
            "id": [4],
            "name": ["Dave"],
            "score": [300],
            "email": ["dave@example.com"],
        })
        table.append(new_data)
        print_table(table, "After inserting row with email")

        # --- 3. Rename a column ---
        print("\n=== Rename column: 'score' -> 'points' ===")
        with table.update_schema() as update:
            update.rename_column("score", "points")

        print_table(table, "After rename")

        # --- 4. Widen a type: INTEGER -> BIGINT ---
        print("\n=== Widen type: 'points' INTEGER -> BIGINT ===")
        with table.update_schema() as update:
            update.update_column("points", BigIntType())

        # Verify existing data is preserved
        print_table(table, "After type widening")

        # Now we can insert large values
        big_score = pa.table({
            "id": [5],
            "name": ["Eve"],
            "points": pa.array([3_000_000_000], type=pa.int64()),
            "email": ["eve@example.com"],
        })
        table.append(big_score)
        print_table(table, "After inserting large value")

        # --- 5. Drop a column ---
        print("\n=== Drop column: 'email' ===")
        with table.update_schema() as update:
            update.drop_column("email")

        print_table(table, "After dropping 'email'")

        # --- 6. Multiple changes in one commit ---
        print("\n=== Multiple changes in one commit ===")
        with table.update_schema() as update:
            update.add_column("rank", StringType())
            update.add_column("active", IntegerType())

        print_table(table, "After adding 'rank' and 'active'")

        # --- 7. Verify final schema ---
        print("\n=== Final schema ===")
        for field in table.schema:
            print(f"  {field.name}: {field.field_type} ({'required' if field.required else 'optional'})")

        catalog.close()
        print("\nDone.")


if __name__ == "__main__":
    main()
