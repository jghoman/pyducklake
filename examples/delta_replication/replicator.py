"""Append-only Delta Lake -> Ducklake replication engine (POC).

Tails a Delta Lake transaction log and replicates new data into a Ducklake
catalog.  Only append operations (AddFileAction with data_change=True) are
replicated; schema evolution is detected but not handled.
"""

from __future__ import annotations

import json
import os
import signal
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from delta_log import (
    AddFileAction,
    MetaDataAction,
    extract_schema,
    list_versions,
    parse_log_file,
    version_path,
)
from pyducklake import Catalog, Schema, optional, required
from pyducklake.types import (
    BigIntType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    SmallIntType,
    StringType,
    TimestampTZType,
    TimestampType,
    TinyIntType,
)

# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

shutdown = False


def handle_signal(signum: int, frame: object) -> None:
    global shutdown
    shutdown = True


signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[replicator {ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Replicator
# ---------------------------------------------------------------------------


class DeltaReplicator:
    """Replicates appends from a Delta Lake table into a Ducklake catalog."""

    def __init__(
        self,
        delta_table_path: str,
        ducklake_catalog: Catalog,
        ducklake_table_name: str,
        poll_interval: float = 3.0,
    ) -> None:
        self.delta_table_path = delta_table_path
        self.catalog = ducklake_catalog
        self.table_name = ducklake_table_name
        self.poll_interval = poll_interval
        self.checkpoint_path = os.path.join(
            delta_table_path, ".ducklake_checkpoint.json"
        )

        self.last_replicated_version: int | None = self._load_checkpoint()

    # -- checkpoint persistence ------------------------------------------------

    def _load_checkpoint(self) -> int | None:
        """Load the last replicated version from the checkpoint file."""
        try:
            with open(self.checkpoint_path, "r") as f:
                data = json.load(f)
            version = data.get("last_replicated_version")
            if version is not None:
                log(f"Resumed from checkpoint: version {version}")
            return version
        except (FileNotFoundError, json.JSONDecodeError, KeyError):
            return None

    def _save_checkpoint(self, version: int) -> None:
        """Persist the last replicated version to disk."""
        data = {
            "last_replicated_version": version,
            "delta_table_path": self.delta_table_path,
        }
        with open(self.checkpoint_path, "w") as f:
            json.dump(data, f)

    # -- schema mapping --------------------------------------------------------

    @staticmethod
    def _delta_schema_to_ducklake(arrow_schema: pa.Schema) -> Schema:
        """Convert a PyArrow schema (from Delta) to a Ducklake Schema."""
        fields = []

        for field in arrow_schema:
            ducklake_type = _arrow_type_to_ducklake(field.type)
            constraint = optional if field.nullable else required
            fields.append(constraint(field.name, ducklake_type))

        return Schema.of(*fields)

    # -- target table ----------------------------------------------------------

    def _init_target_table(self) -> object:
        """Extract schema from version 0, create the Ducklake table."""
        arrow_schema = extract_schema(self.delta_table_path)
        ducklake_schema = self._delta_schema_to_ducklake(arrow_schema)

        log(f"Creating target table '{self.table_name}' with {len(arrow_schema)} columns")
        table = self.catalog.create_table_if_not_exists(
            self.table_name, ducklake_schema
        )
        return table

    # -- version processing ----------------------------------------------------

    def replicate_version(self, version: int) -> int:
        """Process a single Delta version. Returns the number of rows replicated."""
        path = version_path(self.delta_table_path, version)
        actions = parse_log_file(path)

        rows_replicated = 0
        table = self.catalog.load_table(self.table_name)

        for action in actions:
            if isinstance(action, MetaDataAction) and version > 0:
                log(
                    f"WARNING: schema change detected in version {version} "
                    f"— not handled in this POC"
                )

            if isinstance(action, AddFileAction) and action.data_change:
                parquet_path = os.path.join(self.delta_table_path, action.path)
                arrow_table = pq.read_table(parquet_path)
                table.append(arrow_table)
                rows_replicated += arrow_table.num_rows
                log(
                    f"  v{version}: appended {arrow_table.num_rows} rows "
                    f"from {action.path}"
                )

        self.last_replicated_version = version
        self._save_checkpoint(version)
        return rows_replicated

    # -- main loops ------------------------------------------------------------

    def run_once(self) -> int:
        """Single pass: process all pending versions. Returns total rows."""
        self._init_target_table()

        versions = list_versions(self.delta_table_path)
        if not versions:
            log("No versions found in delta log")
            return 0

        total_rows = 0
        for version in versions:
            if (
                self.last_replicated_version is not None
                and version <= self.last_replicated_version
            ):
                continue
            total_rows += self.replicate_version(version)

        log(f"run_once complete: {total_rows} total rows replicated")
        return total_rows

    def run(self) -> None:
        """Main loop: init table, then poll for new versions until shutdown."""
        global shutdown

        self._init_target_table()
        log(
            f"Polling '{self.delta_table_path}' every {self.poll_interval}s "
            f"(last_replicated_version={self.last_replicated_version})"
        )

        while not shutdown:
            versions = list_versions(self.delta_table_path)
            for version in versions:
                if shutdown:
                    break
                if (
                    self.last_replicated_version is not None
                    and version <= self.last_replicated_version
                ):
                    continue
                self.replicate_version(version)

            if not shutdown:
                time.sleep(self.poll_interval)

        log("Shutdown signal received, exiting")


# ---------------------------------------------------------------------------
# Arrow type mapping
# ---------------------------------------------------------------------------

_ARROW_TYPE_MAP: dict[str, object] = {
    "int8": TinyIntType(),
    "int16": SmallIntType(),
    "int32": IntegerType(),
    "int64": BigIntType(),
    "float": FloatType(),
    "double": DoubleType(),
    "string": StringType(),
    "large_string": StringType(),
    "utf8": StringType(),
    "bool": BooleanType(),
    "binary": BinaryType(),
    "date32": DateType(),
}


def _arrow_type_to_ducklake(arrow_type: pa.DataType) -> object:
    """Map a PyArrow DataType to the corresponding Ducklake type instance."""
    type_str = str(arrow_type)

    # Check the static map first.
    if type_str in _ARROW_TYPE_MAP:
        return _ARROW_TYPE_MAP[type_str]

    # Timestamp variants.
    if pa.types.is_timestamp(arrow_type):
        if arrow_type.tz is not None:  # type: ignore[union-attr]
            return TimestampTZType()
        return TimestampType()

    # Decimal.
    if pa.types.is_decimal(arrow_type):
        return DecimalType(arrow_type.precision, arrow_type.scale)  # type: ignore[union-attr]

    log(f"WARNING: unmapped Arrow type {arrow_type!r}, falling back to StringType")
    return StringType()
