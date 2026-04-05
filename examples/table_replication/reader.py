"""Reader: polls a downstream Ducklake and prints new events as they arrive.

Configured via environment variables:
  DUCKLAKE_POSTGRES  — PostgreSQL connection string
  DUCKLAKE_S3_BUCKET — S3 bucket name
  TEAM_ID            — team identifier (for log prefix)
"""

import os
import signal
import sys
import time
from datetime import datetime, timezone

from pyducklake import Catalog, Schema, required, IntegerType, StringType, TimestampTZType

# -- Configuration -----------------------------------------------------------

POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "5"))
POSTGRES_URI = os.environ.get("DUCKLAKE_POSTGRES", "")
S3_BUCKET = os.environ.get("DUCKLAKE_S3_BUCKET", "")
TEAM_ID = os.environ.get("TEAM_ID", "???")

# -- Helpers -----------------------------------------------------------------

shutdown = False


def handle_signal(signum: int, frame: object) -> None:
    global shutdown
    shutdown = True


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[reader-{TEAM_ID} {ts}] {msg}", flush=True)


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


def wait_for_table(max_retries: int = 120) -> Catalog:
    """Wait for the downstream catalog and events table to become available."""
    for attempt in range(max_retries):
        try:
            catalog = Catalog(
                f"team{TEAM_ID}",
                POSTGRES_URI,
                data_path=f"s3://{S3_BUCKET}/",
                properties=make_s3_props(),
            )
            if catalog.table_exists("events"):
                return catalog
            catalog.close()
        except Exception:
            pass
        if shutdown:
            sys.exit(0)
        if attempt % 10 == 0:
            log(f"waiting for events table... (attempt {attempt + 1})")
        time.sleep(2)
    log("FATAL: events table not available after max retries")
    sys.exit(1)


def main() -> None:
    if not POSTGRES_URI or not S3_BUCKET:
        log("FATAL: DUCKLAKE_POSTGRES and DUCKLAKE_S3_BUCKET must be set")
        sys.exit(1)

    log("starting")

    catalog = wait_for_table()
    table = catalog.load_table("events")
    log(f"connected to Ducklake (bucket={S3_BUCKET})")

    last_snapshot_id: int | None = None
    total_seen = 0

    try:
        while not shutdown:
            snap = table.current_snapshot()
            if snap is None:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            current_id = snap.snapshot_id

            if last_snapshot_id is not None and current_id <= last_snapshot_id:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            if last_snapshot_id is None:
                # First poll: read everything
                result = table.scan().to_arrow()
            else:
                # Read only new insertions
                try:
                    changeset = table.table_insertions(
                        start_snapshot=last_snapshot_id,
                        end_snapshot=current_id,
                    )
                    result = changeset.to_arrow()
                except Exception as e:
                    log(f"CDC read error: {e}")
                    time.sleep(POLL_INTERVAL_SECONDS)
                    continue

            last_snapshot_id = current_id

            if result.num_rows == 0:
                continue

            total_seen += result.num_rows

            # Print summary of new events
            event_types: dict[str, int] = {}
            for row in result.to_pylist():
                et = row.get("event_type", "unknown")
                event_types[et] = event_types.get(et, 0) + 1

            type_summary = ", ".join(f"{k}={v}" for k, v in sorted(event_types.items()))
            log(f"+{result.num_rows} events (total: {total_seen}) — {type_summary}")

            # Print a few sample rows
            sample_size = min(3, result.num_rows)
            for row in result.to_pylist()[:sample_size]:
                log(f"  {row['event_type']:12s} user={row['user_id']}  url={row['url']}")

            time.sleep(POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        pass
    finally:
        catalog.close()
        log(f"shutdown — saw {total_seen} events total")


if __name__ == "__main__":
    main()
