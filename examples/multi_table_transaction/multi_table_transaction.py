"""Multi-table transactions: atomic writes across tables with rollback.

Demonstrates:
1. Creating two related tables (orders and order_items)
2. Writing to both atomically in a single transaction
3. Showing rollback behavior when an error occurs mid-transaction
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
)


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        meta_path = str(Path(tmpdir) / "meta.duckdb")
        data_path = str(Path(tmpdir) / "data")

        catalog = Catalog("shop", meta_path, data_path=data_path)

        # --- 1. Create related tables ---
        print("=== Creating tables ===")
        orders_schema = Schema.of(
            required("order_id", IntegerType()),
            required("order_date", DateType()),
            optional("customer", StringType()),
            optional("total", DoubleType()),
        )
        items_schema = Schema.of(
            required("item_id", IntegerType()),
            required("order_id", IntegerType()),
            optional("product", StringType()),
            optional("quantity", IntegerType()),
            optional("price", DoubleType()),
        )
        orders = catalog.create_table("orders", orders_schema)
        items = catalog.create_table("order_items", items_schema)
        print("Created: orders, order_items")

        # --- 2. Atomic multi-table write ---
        print("\n=== Transaction 1: atomic write to both tables ===")
        with catalog.begin_transaction() as txn:
            t_orders = txn.load_table("orders")
            t_items = txn.load_table("order_items")

            order_data = pa.table({
                "order_id": [1, 2],
                "order_date": [date(2024, 3, 1), date(2024, 3, 1)],
                "customer": ["Alice", "Bob"],
                "total": [74.97, 24.99],
            })
            t_orders.append(order_data)

            items_data = pa.table({
                "item_id": [1, 2, 3, 4],
                "order_id": [1, 1, 1, 2],
                "product": ["Widget", "Gadget", "Doohickey", "Gadget"],
                "quantity": [2, 1, 3, 1],
                "price": [9.99, 24.99, 4.99, 24.99],
            })
            t_items.append(items_data)
            # Commits automatically on clean exit

        # Reload tables to see committed data
        orders = catalog.load_table("orders")
        items = catalog.load_table("order_items")

        print(f"Orders: {orders.scan().count()} rows")
        print(orders.scan().to_arrow().to_pandas().to_string(index=False))
        print(f"\nOrder items: {items.scan().count()} rows")
        print(items.scan().to_arrow().to_pandas().to_string(index=False))

        # --- 3. Rollback on error ---
        print("\n=== Transaction 2: demonstrating rollback ===")
        orders_before = orders.scan().count()
        items_before = items.scan().count()

        try:
            with catalog.begin_transaction() as txn:
                t_orders = txn.load_table("orders")
                t_items = txn.load_table("order_items")

                # Write to orders
                bad_order = pa.table({
                    "order_id": [3],
                    "order_date": [date(2024, 3, 2)],
                    "customer": ["Carol"],
                    "total": [999.99],
                })
                t_orders.append(bad_order)

                # Simulate an error before writing items
                raise ValueError("Payment declined!")

        except ValueError as e:
            print(f"Caught error: {e}")
            print("Transaction rolled back.")

        # Verify nothing changed
        orders = catalog.load_table("orders")
        items = catalog.load_table("order_items")
        orders_after = orders.scan().count()
        items_after = items.scan().count()
        print(f"Orders before: {orders_before}, after: {orders_after} (unchanged)")
        print(f"Items before: {items_before}, after: {items_after} (unchanged)")

        # --- 4. Another successful transaction ---
        print("\n=== Transaction 3: another successful multi-table write ===")
        with catalog.begin_transaction() as txn:
            t_orders = txn.load_table("orders")
            t_items = txn.load_table("order_items")

            order_data = pa.table({
                "order_id": [3],
                "order_date": [date(2024, 3, 2)],
                "customer": ["Carol"],
                "total": [149.50],
            })
            t_orders.append(order_data)

            items_data = pa.table({
                "item_id": [5, 6],
                "order_id": [3, 3],
                "product": ["Thingamajig", "Widget"],
                "quantity": [1, 5],
                "price": [99.99, 9.99],
            })
            t_items.append(items_data)

        orders = catalog.load_table("orders")
        items = catalog.load_table("order_items")
        print(f"Orders: {orders.scan().count()} rows")
        print(f"Items: {items.scan().count()} rows")

        catalog.close()
        print("\nDone.")


if __name__ == "__main__":
    main()
