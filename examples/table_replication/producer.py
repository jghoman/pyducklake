"""Producer: generates analytics events and writes them to Ducklake A (source).

Runs continuously, flushing batches at a configurable interval.
"""

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import pyarrow as pa

from pyducklake import Catalog, Schema, required, IntegerType, StringType, TimestampTZType

# -- Configuration -----------------------------------------------------------

EVENTS_PER_SECOND = int(os.environ.get("EVENTS_PER_SECOND", "10"))
FLUSH_INTERVAL_SECONDS = int(os.environ.get("FLUSH_INTERVAL_SECONDS", "5"))

POSTGRES_URI = "postgres:dbname=ducklake host=postgres-source user=ducklake password=ducklake"
S3_BUCKET = "source"

EVENT_TYPES = ["pageview", "click", "form_submit", "purchase", "signup", "api_call"]
URLS = ["/", "/dashboard", "/settings", "/billing", "/docs", "/api", "/login", "/signup", "/profile", "/search"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
OS_TYPES = ["macOS", "Windows", "Linux", "iOS", "Android"]

# Pool of ~200 user IDs, drawn with power-law distribution
USER_POOL = [f"user-{i:04d}" for i in range(200)]
USER_WEIGHTS = [1.0 / (i + 1) ** 0.8 for i in range(200)]

# -- Helpers -----------------------------------------------------------------

shutdown = False


def handle_signal(signum: int, frame: object) -> None:
    global shutdown
    shutdown = True


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[producer {ts}] {msg}", flush=True)


def random_team_id() -> int:
    r = random.random()
    if r < 0.05:
        return 123
    if r < 0.08:
        return 456
    return random.randint(100, 999)


def make_s3_props() -> dict[str, str]:
    return {
        "s3_endpoint": os.environ.get("S3_ENDPOINT", "minio:9000"),
        "s3_access_key_id": os.environ.get("S3_ACCESS_KEY_ID", "minioadmin"),
        "s3_secret_access_key": os.environ.get("S3_SECRET_ACCESS_KEY", "minioadmin"),
        "s3_use_ssl": os.environ.get("S3_USE_SSL", "false"),
        "s3_url_style": os.environ.get("S3_URL_STYLE", "path"),
    }


def generate_event() -> dict[str, object]:
    event_type = random.choice(EVENT_TYPES)
    properties: dict[str, str] = {
        "browser": random.choice(BROWSERS),
        "os": random.choice(OS_TYPES),
        "referrer": random.choice(["google", "direct", "twitter", "github", "email", ""]),
    }
    if event_type == "purchase":
        properties["amount"] = f"{random.uniform(5.0, 500.0):.2f}"
        properties["currency"] = "USD"
    elif event_type == "click":
        properties["element"] = random.choice(["button", "link", "nav", "card", "tab"])

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "team_id": random_team_id(),
        "user_id": random.choices(USER_POOL, weights=USER_WEIGHTS, k=1)[0],
        "timestamp": datetime.now(timezone.utc),
        "url": random.choice(URLS),
        "properties": json.dumps(properties),
    }


def events_to_arrow(events: list[dict[str, object]]) -> pa.Table:
    return pa.table({
        "event_id": pa.array([e["event_id"] for e in events], type=pa.string()),
        "event_type": pa.array([e["event_type"] for e in events], type=pa.string()),
        "team_id": pa.array([e["team_id"] for e in events], type=pa.int32()),
        "user_id": pa.array([e["user_id"] for e in events], type=pa.string()),
        "timestamp": pa.array([e["timestamp"] for e in events], type=pa.timestamp("us", tz="UTC")),
        "url": pa.array([e["url"] for e in events], type=pa.string()),
        "properties": pa.array([e["properties"] for e in events], type=pa.string()),
    })


# -- Main --------------------------------------------------------------------

EVENTS_SCHEMA = Schema.of(
    required("event_id", StringType()),
    required("event_type", StringType()),
    required("team_id", IntegerType()),
    required("user_id", StringType()),
    required("timestamp", TimestampTZType()),
    required("url", StringType()),
    required("properties", StringType()),
)


def main() -> None:
    log(f"starting — {EVENTS_PER_SECOND} events/sec, flush every {FLUSH_INTERVAL_SECONDS}s")

    catalog = Catalog("source", POSTGRES_URI, data_path=f"s3://{S3_BUCKET}/", properties=make_s3_props())
    table = catalog.create_table_if_not_exists("events", EVENTS_SCHEMA)
    log("connected to Ducklake A (source)")

    total_events = 0
    start_time = time.monotonic()

    try:
        while not shutdown:
            batch: list[dict[str, object]] = []
            batch_start = time.monotonic()

            # Generate events for one flush interval
            while time.monotonic() - batch_start < FLUSH_INTERVAL_SECONDS and not shutdown:
                batch.append(generate_event())
                # Throttle to approximate target rate
                if len(batch) % EVENTS_PER_SECOND == 0:
                    elapsed = time.monotonic() - batch_start
                    expected = len(batch) / EVENTS_PER_SECOND
                    if expected > elapsed:
                        time.sleep(expected - elapsed)

            if not batch:
                continue

            arrow_batch = events_to_arrow(batch)
            table.append(arrow_batch)
            total_events += len(batch)

            elapsed = time.monotonic() - start_time
            rate = total_events / elapsed if elapsed > 0 else 0
            team_123 = sum(1 for e in batch if e["team_id"] == 123)
            team_456 = sum(1 for e in batch if e["team_id"] == 456)
            log(f"flushed {len(batch)} events (total: {total_events}, rate: {rate:.1f}/s, "
                f"team-123: {team_123}, team-456: {team_456})")

    except KeyboardInterrupt:
        pass
    finally:
        catalog.close()
        log(f"shutdown — produced {total_events} events total")


if __name__ == "__main__":
    main()
