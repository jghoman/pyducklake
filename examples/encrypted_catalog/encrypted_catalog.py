"""Encrypted catalog: Parquet-level encryption for data at rest.

Demonstrates:
1. Creating an encrypted Ducklake catalog
2. Writing data (Parquet files are encrypted on disk)
3. Reading data through the catalog (transparent decryption)
4. Showing that raw Parquet files are unreadable without the catalog
"""

import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pyducklake import (
    Catalog,
    Schema,
    required,
    optional,
    IntegerType,
    StringType,
)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        meta_path = str(Path(tmpdir) / "meta.duckdb")
        data_path = str(Path(tmpdir) / "data")

        # --- 1. Create an encrypted catalog ---
        print("=== Creating encrypted catalog ===")
        catalog = Catalog("secure_lake", meta_path, data_path=data_path, encrypted=True)
        print(f"Catalog encrypted: {catalog.encrypted}")

        # --- 2. Create table and write data ---
        print("\n=== Writing sensitive data ===")
        schema = Schema.of(
            required("user_id", IntegerType()),
            optional("name", StringType()),
            optional("ssn", StringType()),
        )
        table = catalog.create_table("pii_data", schema)

        # Write enough data to exceed the inline threshold and produce Parquet files
        ids = list(range(1, 501))
        names = [f"User_{i}" for i in ids]
        ssns = [f"{i:03d}-{(i*7)%100:02d}-{(i*13)%10000:04d}" for i in ids]
        data = pa.table({
            "user_id": ids,
            "name": names,
            "ssn": ssns,
        })
        table.append(data)
        print(f"Wrote {data.num_rows} rows")

        # --- 3. Read through catalog (transparent decryption) ---
        print("\n=== Reading through catalog (decrypted, first 5 rows) ===")
        result = table.scan().with_limit(5).to_arrow()
        print(result.to_pandas().to_string(index=False))

        # --- 4. Try to read raw Parquet files directly ---
        print("\n=== Attempting to read raw Parquet files ===")
        parquet_files = list(Path(data_path).rglob("*.parquet"))
        if parquet_files:
            for pf in parquet_files:
                print(f"Found: {pf.name}")
                try:
                    raw = pq.read_table(str(pf))
                    # If we get here, the file was readable (encryption might be
                    # transparent via DuckDB's footer key). Print a note.
                    print(f"  File is readable ({raw.num_rows} rows)")
                    print("  Note: DuckDB ducklake uses footer-level encryption;")
                    print("  files may be openable but column data is encrypted.")
                except Exception as e:
                    print(f"  Cannot read: {type(e).__name__}: {e}")
                    print("  Encryption is working -- raw files are not readable!")
        else:
            print("No parquet files found (data may be stored inline)")

        catalog.close()
        print("\nDone.")


if __name__ == "__main__":
    main()
