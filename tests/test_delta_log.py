"""Tests for the Delta Lake transaction log parser."""

from __future__ import annotations

import json
import os
import sys

import pyarrow as pa

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "examples", "delta_replication"))

from delta_log import (
    AddFileAction,
    CommitInfoAction,
    MetaDataAction,
    ProtocolAction,
    RemoveFileAction,
    extract_schema,
    get_latest_version,
    list_versions,
    parse_action,
    parse_log_file,
    spark_type_to_arrow,
    version_path,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def write_ndjson(path: str, actions_list: list[dict]) -> None:
    """Write a list of dicts as newline-delimited JSON."""
    with open(path, "w") as f:
        for action in actions_list:
            f.write(json.dumps(action) + "\n")


def create_delta_log(tmp_path, versions: dict[int, list[dict]]) -> str:
    """Create a _delta_log/ directory with version JSON files.

    Returns the path to the table root (parent of _delta_log).
    """
    log_dir = os.path.join(tmp_path, "_delta_log")
    os.makedirs(log_dir, exist_ok=True)
    for ver, actions in versions.items():
        write_ndjson(os.path.join(log_dir, f"{ver:020d}.json"), actions)
    return str(tmp_path)


# ---------------------------------------------------------------------------
# Fixtures / sample data
# ---------------------------------------------------------------------------

SAMPLE_ADD = {
    "add": {
        "path": "part-00000.parquet",
        "partitionValues": {},
        "size": 1024,
        "modificationTime": 1700000000000,
        "dataChange": True,
        "stats": '{"numRecords":10}',
    }
}

SAMPLE_REMOVE = {
    "remove": {
        "path": "part-00000.parquet",
        "deletionTimestamp": 1700000001000,
        "dataChange": True,
    }
}

SAMPLE_METADATA = {
    "metaData": {
        "id": "test-table-id",
        "format": {"provider": "parquet", "options": {}},
        "schemaString": json.dumps(
            {
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "long", "nullable": False, "metadata": {}},
                    {"name": "name", "type": "string", "nullable": True, "metadata": {}},
                ],
            }
        ),
        "partitionColumns": [],
        "configuration": {},
        "createdTime": 1700000000000,
    }
}

SAMPLE_COMMIT_INFO = {
    "commitInfo": {
        "timestamp": 1700000000000,
        "operation": "WRITE",
        "operationParameters": {"mode": "Append"},
    }
}

SAMPLE_PROTOCOL = {
    "protocol": {
        "minReaderVersion": 1,
        "minWriterVersion": 2,
    }
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_version_path():
    assert version_path("/data/my_table", 0) == "/data/my_table/_delta_log/00000000000000000000.json"
    assert version_path("/data/my_table", 5) == "/data/my_table/_delta_log/00000000000000000005.json"
    assert version_path("/data/my_table", 123) == "/data/my_table/_delta_log/00000000000000000123.json"


def test_parse_action_add():
    action = parse_action(SAMPLE_ADD)
    assert isinstance(action, AddFileAction)
    assert action.path == "part-00000.parquet"
    assert action.size == 1024
    assert action.data_change is True


def test_parse_action_remove():
    action = parse_action(SAMPLE_REMOVE)
    assert isinstance(action, RemoveFileAction)
    assert action.path == "part-00000.parquet"
    assert action.data_change is True


def test_parse_action_metadata():
    action = parse_action(SAMPLE_METADATA)
    assert isinstance(action, MetaDataAction)
    assert action.id == "test-table-id"
    assert action.partition_columns == []


def test_parse_action_commit_info():
    action = parse_action(SAMPLE_COMMIT_INFO)
    assert isinstance(action, CommitInfoAction)
    assert action.operation == "WRITE"
    assert action.timestamp == 1700000000000


def test_parse_action_protocol():
    action = parse_action(SAMPLE_PROTOCOL)
    assert isinstance(action, ProtocolAction)
    assert action.min_reader_version == 1
    assert action.min_writer_version == 2


def test_parse_action_unknown():
    result = parse_action({"txn": {"appId": "abc", "version": 1}})
    assert result is None


def test_parse_log_file(tmp_path):
    log_file = os.path.join(tmp_path, "00000000000000000000.json")
    write_ndjson(log_file, [SAMPLE_PROTOCOL, SAMPLE_METADATA, SAMPLE_ADD, SAMPLE_COMMIT_INFO])

    actions = parse_log_file(log_file)
    assert len(actions) == 4
    assert isinstance(actions[0], ProtocolAction)
    assert isinstance(actions[1], MetaDataAction)
    assert isinstance(actions[2], AddFileAction)
    assert isinstance(actions[3], CommitInfoAction)


def test_list_versions(tmp_path):
    table_path = create_delta_log(
        tmp_path,
        {
            0: [SAMPLE_PROTOCOL, SAMPLE_METADATA],
            1: [SAMPLE_ADD],
            3: [SAMPLE_ADD],
        },
    )
    versions = list_versions(table_path)
    assert versions == [0, 1, 3]


def test_list_versions_empty(tmp_path):
    log_dir = os.path.join(tmp_path, "_delta_log")
    os.makedirs(log_dir)
    assert list_versions(str(tmp_path)) == []


def test_get_latest_version(tmp_path):
    table_path = create_delta_log(
        tmp_path,
        {
            0: [SAMPLE_PROTOCOL],
            1: [SAMPLE_ADD],
            5: [SAMPLE_ADD],
        },
    )
    assert get_latest_version(table_path) == 5


def test_get_latest_version_empty(tmp_path):
    log_dir = os.path.join(tmp_path, "_delta_log")
    os.makedirs(log_dir)
    assert get_latest_version(str(tmp_path)) is None


def test_spark_type_to_arrow():
    assert spark_type_to_arrow("integer") == pa.int32()
    assert spark_type_to_arrow("long") == pa.int64()
    assert spark_type_to_arrow("string") == pa.large_string()
    assert spark_type_to_arrow("double") == pa.float64()
    assert spark_type_to_arrow("float") == pa.float32()
    assert spark_type_to_arrow("boolean") == pa.bool_()
    assert spark_type_to_arrow("short") == pa.int16()
    assert spark_type_to_arrow("byte") == pa.int8()
    assert spark_type_to_arrow("binary") == pa.binary()
    assert spark_type_to_arrow("date") == pa.date32()
    assert spark_type_to_arrow("timestamp") == pa.timestamp("us")
    assert spark_type_to_arrow("decimal(10,2)") == pa.decimal128(10, 2)


def test_extract_schema(tmp_path):
    schema_string = json.dumps(
        {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": False, "metadata": {}},
                {"name": "name", "type": "string", "nullable": True, "metadata": {}},
                {"name": "price", "type": "decimal(10,2)", "nullable": True, "metadata": {}},
                {"name": "active", "type": "boolean", "nullable": False, "metadata": {}},
            ],
        }
    )
    metadata_action = {
        "metaData": {
            "id": "test-id",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": schema_string,
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1700000000000,
        }
    }
    table_path = create_delta_log(
        tmp_path,
        {
            0: [SAMPLE_PROTOCOL, metadata_action, SAMPLE_COMMIT_INFO],
        },
    )

    schema = extract_schema(table_path)
    assert isinstance(schema, pa.Schema)
    assert len(schema) == 4
    assert schema.field("id").type == pa.int64()
    assert schema.field("id").nullable is False
    assert schema.field("name").type == pa.large_string()
    assert schema.field("name").nullable is True
    assert schema.field("price").type == pa.decimal128(10, 2)
    assert schema.field("active").type == pa.bool_()
    assert schema.field("active").nullable is False
