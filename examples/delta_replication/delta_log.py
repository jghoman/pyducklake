"""Pure-Python parser for Delta Lake transaction log files (_delta_log/*.json)."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass

import pyarrow as pa


# ---------------------------------------------------------------------------
# Action dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class AddFileAction:
    path: str
    partition_values: dict[str, str]
    size: int
    modification_time: int
    data_change: bool


@dataclass(frozen=True, slots=True)
class RemoveFileAction:
    path: str
    deletion_timestamp: int | None
    data_change: bool


@dataclass(frozen=True, slots=True)
class MetaDataAction:
    id: str
    schema_string: str
    partition_columns: list[str]
    configuration: dict[str, str]
    created_time: int | None


@dataclass(frozen=True, slots=True)
class CommitInfoAction:
    version: int | None
    timestamp: int
    operation: str
    operation_parameters: dict[str, str]


@dataclass(frozen=True, slots=True)
class ProtocolAction:
    min_reader_version: int
    min_writer_version: int


Action = AddFileAction | RemoveFileAction | MetaDataAction | CommitInfoAction | ProtocolAction


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def version_path(table_path: str, version: int) -> str:
    """Return the path to the JSON log file for *version* (zero-padded to 20 digits)."""
    return os.path.join(table_path, "_delta_log", f"{version:020d}.json")


def list_versions(table_path: str) -> list[int]:
    """List all version numbers in ``_delta_log/``, sorted ascending."""
    log_dir = os.path.join(table_path, "_delta_log")
    try:
        entries = os.listdir(log_dir)
    except FileNotFoundError:
        return []
    versions: list[int] = []
    for entry in entries:
        if entry.endswith(".json"):
            try:
                versions.append(int(entry.removesuffix(".json")))
            except ValueError:
                continue
    versions.sort()
    return versions


def get_latest_version(table_path: str) -> int | None:
    """Return the highest version number, or ``None`` if no versions exist."""
    versions = list_versions(table_path)
    return versions[-1] if versions else None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

_DECIMAL_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")


def spark_type_to_arrow(spark_type: str) -> pa.DataType:
    """Convert a Spark type string to a PyArrow DataType."""
    mapping: dict[str, pa.DataType] = {
        "integer": pa.int32(),
        "long": pa.int64(),
        "string": pa.large_string(),
        "double": pa.float64(),
        "float": pa.float32(),
        "boolean": pa.bool_(),
        "short": pa.int16(),
        "byte": pa.int8(),
        "binary": pa.binary(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us"),
        "timestamp_ntz": pa.timestamp("us"),
    }
    if spark_type in mapping:
        return mapping[spark_type]

    m = _DECIMAL_RE.match(spark_type)
    if m:
        return pa.decimal128(int(m.group(1)), int(m.group(2)))

    raise ValueError(f"Unsupported Spark type: {spark_type!r}")


def parse_action(obj: dict) -> Action | None:
    """Parse a single JSON object (one NDJSON line) into an Action. Returns None for unknown types."""
    if "add" in obj:
        a = obj["add"]
        return AddFileAction(
            path=a["path"],
            partition_values=a.get("partitionValues", {}),
            size=a["size"],
            modification_time=a["modificationTime"],
            data_change=a.get("dataChange", True),
        )
    if "remove" in obj:
        r = obj["remove"]
        return RemoveFileAction(
            path=r["path"],
            deletion_timestamp=r.get("deletionTimestamp"),
            data_change=r.get("dataChange", True),
        )
    if "metaData" in obj:
        m = obj["metaData"]
        return MetaDataAction(
            id=m["id"],
            schema_string=m["schemaString"],
            partition_columns=m.get("partitionColumns", []),
            configuration=m.get("configuration", {}),
            created_time=m.get("createdTime"),
        )
    if "commitInfo" in obj:
        c = obj["commitInfo"]
        return CommitInfoAction(
            version=c.get("version"),
            timestamp=c["timestamp"],
            operation=c.get("operation", ""),
            operation_parameters=c.get("operationParameters", {}),
        )
    if "protocol" in obj:
        p = obj["protocol"]
        return ProtocolAction(
            min_reader_version=p["minReaderVersion"],
            min_writer_version=p["minWriterVersion"],
        )
    return None


def parse_log_file(path: str) -> list[Action]:
    """Read an NDJSON log file and return a list of parsed Action objects."""
    actions: list[Action] = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            action = parse_action(obj)
            if action is not None:
                actions.append(action)
    return actions


def extract_schema(table_path: str) -> pa.Schema:
    """Read version 0's metaData action and convert its schemaString to a PyArrow schema."""
    path = version_path(table_path, 0)
    actions = parse_log_file(path)
    metadata = None
    for action in actions:
        if isinstance(action, MetaDataAction):
            metadata = action
            break
    if metadata is None:
        raise ValueError(f"No metaData action found in {path}")

    struct = json.loads(metadata.schema_string)
    fields: list[pa.Field] = []
    for field in struct["fields"]:
        arrow_type = spark_type_to_arrow(field["type"])
        fields.append(pa.field(field["name"], arrow_type, nullable=field.get("nullable", True)))
    return pa.schema(fields)
