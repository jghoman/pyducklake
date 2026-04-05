"""Quickstart: create a catalog, define a table, write data, read it back.

This is the minimal hello-world example for pyducklake. It uses an
in-memory DuckDB metadata store and a temporary directory for data files,
so it runs with zero external dependencies beyond pyducklake itself.
"""

import tempfile
from pathlib import Path

import pyarrow as pa

from pyducklake import Catalog, Schema, required, optional, IntegerType, StringType


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        meta_path = str(Path(tmpdir) / "meta.duckdb")
        data_path = str(Path(tmpdir) / "data")

        # --- 1. Create a catalog ---
        print("=== Creating catalog ===")
        catalog = Catalog("my_lake", meta_path, data_path=data_path)
        print(f"Catalog: {catalog.name}")

        # --- 2. Define a schema ---
        print("\n=== Defining schema ===")
        schema = Schema.of(
            required("id", IntegerType()),
            optional("name", StringType()),
            optional("email", StringType()),
        )
        print(f"Schema fields: {schema.column_names()}")

        # --- 3. Create a table ---
        print("\n=== Creating table ===")
        table = catalog.create_table("users", schema)
        print(f"Table: {table.namespace}.{table.name}")

        # --- 4. Write data ---
        print("\n=== Writing data ===")
        data = pa.table({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "email": ["alice@example.com", "bob@example.com", "carol@example.com"],
        })
        table.append(data)
        print(f"Inserted {data.num_rows} rows")

        # --- 5. Read data back ---
        print("\n=== Reading all data ===")
        result = table.scan().to_arrow()
        print(result.to_pandas().to_string(index=False))

        # --- 6. Filtered scan ---
        print("\n=== Filtered scan (id > 1) ===")
        result = table.scan("id > 1").to_arrow()
        print(result.to_pandas().to_string(index=False))

        # --- 7. Column projection ---
        print("\n=== Projected scan (name only) ===")
        result = table.scan().select("name").to_arrow()
        print(result.to_pandas().to_string(index=False))

        # --- 8. Row count ---
        count = table.scan().count()
        print(f"\nTotal rows: {count}")

        catalog.close()
        print("\nDone.")


if __name__ == "__main__":
    main()
