"""Service entry point: runs the Delta -> Ducklake replicator."""

import os
import sys
import time

from pyducklake import Catalog
from replicator import DeltaReplicator, log

DELTA_TABLE_PATH = os.environ.get("DELTA_TABLE_PATH", "/data/delta/events")
DUCKLAKE_POSTGRES = os.environ.get("DUCKLAKE_POSTGRES", "postgres:dbname=ducklake host=postgres user=ducklake password=ducklake")
DUCKLAKE_S3_BUCKET = os.environ.get("DUCKLAKE_S3_BUCKET", "ducklake-target")
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL_SECONDS", "3"))


def make_s3_props() -> dict[str, str]:
    return {
        "s3_endpoint": os.environ.get("S3_ENDPOINT", "minio:9000"),
        "s3_access_key_id": os.environ.get("S3_ACCESS_KEY_ID", "minioadmin"),
        "s3_secret_access_key": os.environ.get("S3_SECRET_ACCESS_KEY", "minioadmin"),
        "s3_use_ssl": os.environ.get("S3_USE_SSL", "false"),
        "s3_url_style": os.environ.get("S3_URL_STYLE", "path"),
    }


def wait_for_delta_table(max_retries: int = 120) -> None:
    for attempt in range(max_retries):
        if os.path.exists(os.path.join(DELTA_TABLE_PATH, "_delta_log")):
            return
        log(f"waiting for Delta table at {DELTA_TABLE_PATH}... (attempt {attempt + 1}/{max_retries})")
        time.sleep(2)
    log("FATAL: Delta table not available after max retries")
    sys.exit(1)


def main() -> None:
    log("starting Delta -> Ducklake replicator")

    wait_for_delta_table()

    catalog = Catalog(
        "target",
        DUCKLAKE_POSTGRES,
        data_path=f"s3://{DUCKLAKE_S3_BUCKET}/",
        properties=make_s3_props(),
    )
    log(f"connected to Ducklake (postgres + s3://{DUCKLAKE_S3_BUCKET}/)")

    replicator = DeltaReplicator(
        delta_table_path=DELTA_TABLE_PATH,
        ducklake_catalog=catalog,
        ducklake_table_name="events",
        poll_interval=POLL_INTERVAL,
    )

    try:
        replicator.run()
    finally:
        catalog.close()
        log("shutdown complete")


if __name__ == "__main__":
    main()
