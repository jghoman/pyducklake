"""ETL pipeline: batch loading, transformations, upsert, schema evolution, and partitioning.

Simulates a realistic ETL workflow where:
1. An events table is created and partitioned by day
2. Data arrives in batches (simulating daily loads)
3. Late-arriving corrections are applied via upsert
4. A new column is added mid-pipeline (schema evolution)
5. Final state is queried
"""

import tempfile
from datetime import date
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
    DateType,
    DAY,
)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        meta_path = str(Path(tmpdir) / "meta.duckdb")
        data_path = str(Path(tmpdir) / "data")

        catalog = Catalog("warehouse", meta_path, data_path=data_path)

        # --- 1. Create table with partitioning ---
        print("=== Creating partitioned events table ===")
        schema = Schema.of(
            required("event_id", IntegerType()),
            required("event_date", DateType()),
            optional("user_id", IntegerType()),
            optional("event_type", StringType()),
            optional("amount", DoubleType()),
        )
        table = catalog.create_table("events", schema)

        # Partition by day on event_date
        with table.update_spec() as spec:
            spec.add_field("event_date", DAY)
        print(f"Partition spec: {table.spec}")

        # --- 2. Batch load: day 1 ---
        print("\n=== Batch 1: 2024-01-15 ===")
        batch1 = pa.table({
            "event_id": [1, 2, 3],
            "event_date": [date(2024, 1, 15)] * 3,
            "user_id": [100, 101, 102],
            "event_type": ["purchase", "signup", "purchase"],
            "amount": [29.99, 0.0, 149.50],
        })
        table.append(batch1)
        print(f"Loaded {batch1.num_rows} rows")

        # --- 3. Batch load: day 2 ---
        print("\n=== Batch 2: 2024-01-16 ===")
        batch2 = pa.table({
            "event_id": [4, 5, 6],
            "event_date": [date(2024, 1, 16)] * 3,
            "user_id": [100, 103, 101],
            "event_type": ["return", "purchase", "purchase"],
            "amount": [-29.99, 75.00, 12.99],
        })
        table.append(batch2)
        print(f"Loaded {batch2.num_rows} rows")

        # --- 4. Upsert: late corrections ---
        print("\n=== Upsert: correcting event_id=2 and adding event_id=7 ===")
        corrections = pa.table({
            "event_id": [2, 7],
            "event_date": [date(2024, 1, 15), date(2024, 1, 16)],
            "user_id": [101, 104],
            "event_type": ["signup", "purchase"],
            "amount": [0.0, 210.00],  # event 2 unchanged, event 7 is new
        })
        result = table.upsert(corrections, join_cols=["event_id"])
        print(f"Upsert result: {result.rows_updated} updated, {result.rows_inserted} inserted")

        # --- 5. Schema evolution: add a column ---
        print("\n=== Schema evolution: adding 'channel' column ===")
        with table.update_schema() as update:
            update.add_column("channel", StringType())

        print(f"New schema: {table.schema.column_names()}")

        # --- 6. Write data with new column ---
        print("\n=== Batch 3: 2024-01-17 (with channel) ===")
        batch3 = pa.table({
            "event_id": [8, 9],
            "event_date": [date(2024, 1, 17)] * 2,
            "user_id": [105, 100],
            "event_type": ["purchase", "purchase"],
            "amount": [55.00, 89.99],
            "channel": ["web", "mobile"],
        })
        table.append(batch3)
        print(f"Loaded {batch3.num_rows} rows")

        # --- 7. Query final state ---
        print("\n=== Final table state ===")
        result = table.scan().to_arrow()
        print(result.to_pandas().to_string(index=False))

        # --- 8. Filtered query: purchases only ---
        print("\n=== Purchases only (amount > 0) ===")
        purchases = table.scan("event_type = 'purchase' AND amount > 0").to_arrow()
        print(purchases.to_pandas().to_string(index=False))

        print(f"\nTotal rows: {table.scan().count()}")
        catalog.close()
        print("Done.")


if __name__ == "__main__":
    main()
