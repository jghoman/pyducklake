"""Router: reads CDC from Ducklake A and routes events to downstream Ducklakes by team_id.

team_id=123 -> Ducklake team-123
team_id=456 -> Ducklake team-456
all others  -> dropped
"""

import os
import signal
import sys
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc

from pyducklake import Catalog, Schema, required, IntegerType, StringType, TimestampTZType

# -- Configuration -----------------------------------------------------------

POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "3"))

SOURCE_POSTGRES = "postgres:dbname=ducklake host=postgres-source user=ducklake password=ducklake"
TEAM_123_POSTGRES = "postgres:dbname=ducklake host=postgres-123 user=ducklake password=ducklake"
TEAM_456_POSTGRES = "postgres:dbname=ducklake host=postgres-456 user=ducklake password=ducklake"

TEAM_IDS = {123, 456}

# -- Helpers -----------------------------------------------------------------

shutdown = False


def handle_signal(signum: int, frame: object) -> None:
    global shutdown
    shutdown = True


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[router  {ts}] {msg}", flush=True)


def make_s3_props() -> dict[str, str]:
    return {
        "s3_endpoint": os.environ.get("S3_ENDPOINT", "minio:9000"),
        "s3_access_key_id": os.environ.get("S3_ACCESS_KEY_ID", "minioadmin"),
        "s3_secret_access_key": os.environ.get("S3_SECRET_ACCESS_KEY", "minioadmin"),
        "s3_use_ssl": os.environ.get("S3_USE_SSL", "false"),
        "s3_url_style": os.environ.get("S3_URL_STYLE", "path"),
    }


EVENTS_SCHEMA = Schema.of(
    required("event_id", StringType()),
    required("event_type", StringType()),
    required("team_id", IntegerType()),
    required("user_id", StringType()),
    required("timestamp", TimestampTZType()),
    required("url", StringType()),
    required("properties", StringType()),
)


# -- Main --------------------------------------------------------------------


def wait_for_source_table(max_retries: int = 60) -> Catalog:
    """Wait for the source catalog and events table to become available."""
    for attempt in range(max_retries):
        try:
            catalog = Catalog("source", SOURCE_POSTGRES, data_path="s3://source/", properties=make_s3_props())
            if catalog.table_exists("events"):
                return catalog
            catalog.close()
        except Exception:
            pass
        if shutdown:
            sys.exit(0)
        log(f"waiting for source table... (attempt {attempt + 1}/{max_retries})")
        time.sleep(2)
    log("FATAL: source table not available after max retries")
    sys.exit(1)


def main() -> None:
    log("starting")

    # Connect to source
    source_catalog = wait_for_source_table()
    source_table = source_catalog.load_table("events")
    log("connected to source Ducklake A")

    # Connect to downstream Ducklakes
    cat_123 = Catalog("team123", TEAM_123_POSTGRES, data_path="s3://team-123/", properties=make_s3_props())
    tbl_123 = cat_123.create_table_if_not_exists("events", EVENTS_SCHEMA)
    log("connected to downstream Ducklake team-123")

    cat_456 = Catalog("team456", TEAM_456_POSTGRES, data_path="s3://team-456/", properties=make_s3_props())
    tbl_456 = cat_456.create_table_if_not_exists("events", EVENTS_SCHEMA)
    log("connected to downstream Ducklake team-456")

    # Track the last processed snapshot
    last_snapshot_id: int | None = None

    # Stats
    total_routed_123 = 0
    total_routed_456 = 0
    total_dropped = 0

    try:
        while not shutdown:
            # Get current snapshot from source
            snap = source_table.current_snapshot()
            if snap is None:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            current_id = snap.snapshot_id

            if last_snapshot_id is not None and current_id <= last_snapshot_id:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            # Read insertions since last processed snapshot
            if last_snapshot_id is None:
                # First poll: read everything up to current snapshot
                # Use snapshot 0 as start (before any data)
                start_snap = 0
            else:
                start_snap = last_snapshot_id

            try:
                changeset = source_table.table_insertions(
                    start_snapshot=start_snap,
                    end_snapshot=current_id,
                    columns=("event_id", "event_type", "team_id", "user_id", "timestamp", "url", "properties"),
                    filter_expr="team_id IN (123, 456)",
                )
            except Exception as e:
                log(f"CDC read error: {e}")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            inserted = changeset.to_arrow()

            if inserted.num_rows > 0:
                # Split by team_id
                mask_123 = pc.equal(inserted.column("team_id"), pa.scalar(123, type=pa.int32()))
                mask_456 = pc.equal(inserted.column("team_id"), pa.scalar(456, type=pa.int32()))

                batch_123 = inserted.filter(mask_123)
                batch_456 = inserted.filter(mask_456)

                if batch_123.num_rows > 0:
                    tbl_123.append(batch_123)
                    total_routed_123 += batch_123.num_rows

                if batch_456.num_rows > 0:
                    tbl_456.append(batch_456)
                    total_routed_456 += batch_456.num_rows

                log(f"snapshot {start_snap}→{current_id}: "
                    f"routed {batch_123.num_rows}→team-123, {batch_456.num_rows}→team-456")

            # Also count dropped (not fetched due to filter, but estimate from source)
            try:
                all_insertions = source_table.table_insertions(
                    start_snapshot=start_snap,
                    end_snapshot=current_id,
                    columns=("event_id",),
                )
                total_in_range = all_insertions.num_rows
                dropped = total_in_range - inserted.num_rows
                total_dropped += dropped
            except Exception:
                dropped = 0

            last_snapshot_id = current_id

            if inserted.num_rows > 0 or dropped > 0:
                log(f"totals: team-123={total_routed_123}, team-456={total_routed_456}, dropped={total_dropped}")

            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        pass
    finally:
        source_catalog.close()
        cat_123.close()
        cat_456.close()
        log(f"shutdown — routed {total_routed_123} to team-123, {total_routed_456} to team-456, "
            f"dropped {total_dropped}")


if __name__ == "__main__":
    main()
