"""Table maintenance: compaction, snapshot expiration, and file cleanup.

Demonstrates:
1. Writing many small batches (creating many small files)
2. Inspecting file count before compaction
3. Compacting files
4. Expiring old snapshots
5. Cleaning up orphaned files
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

        # --- 1. Create table ---
        print("=== Creating table ===")
        schema = Schema.of(
            required("id", IntegerType()),
            optional("name", StringType()),
            optional("value", DoubleType()),
        )
        table = catalog.create_table("metrics", schema)

        # --- 2. Write many small batches ---
        # Each batch is large enough to avoid inlining (produces separate Parquet files)
        print("\n=== Writing 10 small batches ===")
        for i in range(10):
            rows = list(range(i * 500, (i + 1) * 500))
            batch = pa.table({
                "id": rows,
                "name": [f"metric_{r}_{'x' * 200}" for r in rows],
                "value": [float(r) * 1.5 for r in rows],
            })
            table.append(batch)
        print(f"Total rows: {table.scan().count()}")

        # --- 3. Inspect files before compaction ---
        print("\n=== Files before compaction ===")
        files_before = table.inspect().files()
        print(f"Data files: {files_before.num_rows}")

        # --- 4. Inspect snapshots ---
        print("\n=== Snapshots before maintenance ===")
        snapshots = table.inspect().snapshots()
        print(f"Total snapshots: {snapshots.num_rows}")

        # --- 5. Compact files ---
        print("\n=== Compacting files ===")
        maint = table.maintenance()
        maint.compact()

        files_after = table.inspect().files()
        print(f"Data files after compaction: {files_after.num_rows}")

        # Verify data integrity after compaction
        count = table.scan().count()
        print(f"Rows after compaction: {count}")

        # --- 6. Expire old snapshots ---
        print("\n=== Expiring old snapshots (keep 1 version) ===")
        maint.expire_snapshots(versions=1)

        snapshots_after = table.inspect().snapshots()
        print(f"Snapshots after expiration: {snapshots_after.num_rows}")

        # --- 7. Clean up old files ---
        print("\n=== Cleaning up old files ===")
        maint.cleanup_files()
        print("Cleanup complete")

        # --- 8. Delete orphaned files ---
        print("\n=== Deleting orphaned files ===")
        maint.delete_orphaned_files()
        print("Orphan cleanup complete")

        # --- 9. Checkpoint ---
        print("\n=== Running checkpoint ===")
        maint.checkpoint()
        print("Checkpoint complete")

        # --- 10. Verify final state ---
        print(f"\n=== Final state ===")
        print(f"Total rows: {table.scan().count()}")
        print(f"Data files: {table.inspect().files().num_rows}")
        print(f"Snapshots: {table.inspect().snapshots().num_rows}")

        catalog.close()
        print("\nDone.")


if __name__ == "__main__":
    main()
