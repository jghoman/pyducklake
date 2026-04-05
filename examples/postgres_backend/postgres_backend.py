"""PostgreSQL metadata backend: use PostgreSQL instead of DuckDB for catalog metadata.

Demonstrates:
1. Connecting to PostgreSQL as the metadata store
2. All the same operations work identically
3. Multiple catalogs can share the same PostgreSQL instance

Requires the Docker Compose stack in this directory to be running:
    docker compose up -d
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
    StringType,
    DoubleType,
)


def main() -> None:
    # PostgreSQL connection string matching docker-compose.yml
    pg_uri = "postgres:dbname=ducklake host=localhost port=5488 user=ducklake password=ducklake"

    with tempfile.TemporaryDirectory() as tmpdir:
        data_path = str(Path(tmpdir) / "data")

        # --- 1. Create catalog with PostgreSQL backend ---
        print("=== Creating catalog with PostgreSQL metadata backend ===")
        catalog = Catalog("pg_lake", pg_uri, data_path=data_path)
        print(f"Catalog: {catalog.name}")
        print(f"Metadata URI: {pg_uri}")

        # --- 2. Create table ---
        print("\n=== Creating table ===")
        schema = Schema.of(
            required("id", IntegerType()),
            optional("name", StringType()),
            optional("value", DoubleType()),
        )
        table = catalog.create_table("metrics", schema)
        print(f"Table: {table.namespace}.{table.name}")

        # --- 3. Write data ---
        print("\n=== Writing data ===")
        data = pa.table({
            "id": [1, 2, 3, 4, 5],
            "name": ["cpu_usage", "mem_usage", "disk_io", "net_rx", "net_tx"],
            "value": [45.2, 72.8, 1024.5, 500.0, 250.3],
        })
        table.append(data)
        print(f"Inserted {data.num_rows} rows")

        # --- 4. Read data ---
        print("\n=== Reading data ===")
        result = table.scan().to_arrow()
        print(result.to_pandas().to_string(index=False))

        # --- 5. Schema evolution ---
        print("\n=== Schema evolution: add 'unit' column ===")
        with table.update_schema() as update:
            update.add_column("unit", StringType())
        print(f"Schema: {table.schema.column_names()}")

        # --- 6. Upsert with new column ---
        print("\n=== Upserting with units ===")
        updated = pa.table({
            "id": [1, 2, 6],
            "name": ["cpu_usage", "mem_usage", "gpu_usage"],
            "value": [55.0, 68.5, 92.1],
            "unit": ["%", "%", "%"],
        })
        result = table.upsert(updated, join_cols=["id"])
        print(f"Updated: {result.rows_updated}, Inserted: {result.rows_inserted}")

        # --- 7. Final state ---
        print("\n=== Final table state ===")
        result = table.scan().to_arrow()
        print(result.to_pandas().to_string(index=False))

        # --- 8. Clean up ---
        catalog.drop_table("metrics")
        print("\nCleaned up table")

        catalog.close()
        print("Done.")


if __name__ == "__main__":
    main()
