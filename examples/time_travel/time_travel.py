"""Time travel and change data capture (CDC).

Demonstrates:
1. Writing data in multiple commits (creating snapshots)
2. Querying historical state via snapshot IDs
3. Using CDC functions to see what changed between snapshots
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
    with tempfile.TemporaryDirectory() as tmpdir:
        meta_path = str(Path(tmpdir) / "meta.duckdb")
        data_path = str(Path(tmpdir) / "data")

        catalog = Catalog("lake", meta_path, data_path=data_path)

        # --- 1. Create table and insert initial data ---
        print("=== Snapshot 1: initial load ===")
        schema = Schema.of(
            required("product_id", IntegerType()),
            optional("name", StringType()),
            optional("price", DoubleType()),
        )
        table = catalog.create_table("products", schema)

        initial = pa.table({
            "product_id": [1, 2, 3],
            "name": ["Widget", "Gadget", "Doohickey"],
            "price": [9.99, 24.99, 4.99],
        })
        table.append(initial)

        snap1 = table.current_snapshot()
        assert snap1 is not None
        print(f"Snapshot {snap1.snapshot_id} at {snap1.timestamp}")
        print(table.scan().to_arrow().to_pandas().to_string(index=False))

        # --- 2. Update prices ---
        print("\n=== Snapshot 2: price updates ===")
        updates = pa.table({
            "product_id": [1, 2, 3],
            "name": ["Widget", "Gadget", "Doohickey"],
            "price": [12.99, 24.99, 3.99],  # Widget up, Doohickey down, Gadget unchanged
        })
        table.overwrite(updates)

        snap2 = table.current_snapshot()
        assert snap2 is not None
        print(f"Snapshot {snap2.snapshot_id} at {snap2.timestamp}")
        print(table.scan().to_arrow().to_pandas().to_string(index=False))

        # --- 3. Add new products ---
        print("\n=== Snapshot 3: new products ===")
        new_products = pa.table({
            "product_id": [4, 5],
            "name": ["Thingamajig", "Whatchamacallit"],
            "price": [19.99, 7.49],
        })
        table.append(new_products)

        snap3 = table.current_snapshot()
        assert snap3 is not None
        print(f"Snapshot {snap3.snapshot_id} at {snap3.timestamp}")
        print(table.scan().to_arrow().to_pandas().to_string(index=False))

        # --- 4. Delete a product ---
        print("\n=== Snapshot 4: delete Doohickey ===")
        table.delete("product_id = 3")

        snap4 = table.current_snapshot()
        assert snap4 is not None
        print(f"Snapshot {snap4.snapshot_id} at {snap4.timestamp}")
        print(table.scan().to_arrow().to_pandas().to_string(index=False))

        # --- 5. Time travel: read historical state ---
        print("\n=== Time travel: reading snapshot 1 (initial state) ===")
        historical = table.scan().with_snapshot(snap1.snapshot_id).to_arrow()
        print(historical.to_pandas().to_string(index=False))

        print(f"\n=== Time travel: reading snapshot 2 (after price update) ===")
        historical = table.scan().with_snapshot(snap2.snapshot_id).to_arrow()
        print(historical.to_pandas().to_string(index=False))

        # --- 6. CDC: what changed between snapshots ---
        print(f"\n=== CDC: insertions between snapshot {snap2.snapshot_id} and {snap3.snapshot_id} ===")
        insertions = table.table_insertions(snap2.snapshot_id, snap3.snapshot_id)
        print(insertions.to_pandas().to_string(index=False))

        print(f"\n=== CDC: deletions between snapshot {snap3.snapshot_id} and {snap4.snapshot_id} ===")
        deletions = table.table_deletions(snap3.snapshot_id, snap4.snapshot_id)
        print(deletions.to_pandas().to_string(index=False))

        print(f"\n=== CDC: all changes from snapshot {snap1.snapshot_id} to {snap4.snapshot_id} ===")
        changes = table.table_changes(snap1.snapshot_id, snap4.snapshot_id)
        print(changes.to_pandas().to_string(index=False))

        # --- 7. Snapshot history ---
        print("\n=== Snapshot history ===")
        history = table.inspect().history()
        print(history.to_pandas().to_string(index=False))

        catalog.close()
        print("\nDone.")


if __name__ == "__main__":
    main()
