"""Shared fixtures for integration tests using testcontainers."""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Generator

import pytest
from testcontainers.minio import MinioContainer
from testcontainers.mysql import MySqlContainer
from testcontainers.postgres import PostgresContainer

from pyducklake import Catalog, Schema
from pyducklake.types import DoubleType, IntegerType, NestedField, StringType

# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def postgres() -> Generator[PostgresContainer, None, None]:
    """Spin up a PostgreSQL container for the test module."""
    with PostgresContainer("postgres:17", driver=None) as pg:
        _wait_for_postgres(pg)
        yield pg


def _wait_for_postgres(pg: PostgresContainer, timeout: int = 30) -> None:
    """Poll until PostgreSQL accepts connections."""
    import psycopg2  # noqa: PLC0415

    deadline = time.monotonic() + timeout
    host = pg.get_container_host_ip()
    port = pg.get_exposed_port(5432)
    while True:
        try:
            conn = psycopg2.connect(dbname="test", host=host, port=port, user="test", password="test")
            conn.close()
            return
        except Exception:
            if time.monotonic() > deadline:
                raise TimeoutError(f"PostgreSQL not ready within {timeout}s")
            time.sleep(0.5)


def _create_pg_database(pg: PostgresContainer, db_name: str) -> None:
    """Create a fresh database in the PostgreSQL container."""
    import psycopg2  # noqa: PLC0415
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT  # noqa: PLC0415

    host = pg.get_container_host_ip()
    port = pg.get_exposed_port(5432)
    conn = psycopg2.connect(dbname="test", host=host, port=port, user="test", password="test")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute(f'CREATE DATABASE "{db_name}"')
    cur.close()
    conn.close()


def _pg_connection_string(pg: PostgresContainer, db_name: str) -> str:
    """Build DuckDB-compatible postgres connection string."""
    host = pg.get_container_host_ip()
    port = pg.get_exposed_port(5432)
    return f"postgres:dbname={db_name} host={host} port={port} user=test password=test"


# ---------------------------------------------------------------------------
# MinIO (S3-compatible)
# ---------------------------------------------------------------------------

MINIO_BUCKET = "pyducklake-test"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"


@pytest.fixture(scope="module")
def minio() -> Generator[MinioContainer, None, None]:
    """Spin up a MinIO container for the test module."""
    with MinioContainer(
        image="minio/minio:latest",
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
    ) as mc:
        # Create the test bucket
        from minio import Minio  # noqa: PLC0415

        client = Minio(
            mc.get_config()["endpoint"],
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        client.make_bucket(MINIO_BUCKET)
        yield mc


def _s3_properties(mc: MinioContainer) -> dict[str, str]:
    """DuckDB SET properties to configure S3 access to MinIO."""
    config = mc.get_config()
    # endpoint is host:port
    return {
        "s3_endpoint": config["endpoint"],
        "s3_access_key_id": MINIO_ACCESS_KEY,
        "s3_secret_access_key": MINIO_SECRET_KEY,
        "s3_use_ssl": "false",
        "s3_url_style": "path",
    }


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mysql() -> Generator[MySqlContainer, None, None]:
    """Spin up a MySQL container for the test module."""
    with MySqlContainer("mysql:8.0") as mc:
        _wait_for_mysql(mc)
        yield mc


def _wait_for_mysql(mc: MySqlContainer, timeout: int = 60) -> None:
    """Poll until MySQL accepts connections."""
    import pymysql  # noqa: PLC0415

    deadline = time.monotonic() + timeout
    host = mc.get_container_host_ip()
    port = int(mc.get_exposed_port(3306))
    while True:
        try:
            conn = pymysql.connect(host=host, port=port, user="test", password="test", database="test")
            conn.close()
            return
        except Exception:
            if time.monotonic() > deadline:
                raise TimeoutError(f"MySQL not ready within {timeout}s")
            time.sleep(0.5)


def _create_mysql_database(mc: MySqlContainer, db_name: str) -> None:
    """Create a fresh database in the MySQL container (using root)."""
    import pymysql  # noqa: PLC0415

    host = mc.get_container_host_ip()
    port = int(mc.get_exposed_port(3306))
    conn = pymysql.connect(host=host, port=port, user="root", password=mc.root_password)
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE `{db_name}`")
    cur.close()
    conn.close()


def _mysql_connection_string(mc: MySqlContainer, db_name: str) -> str:
    """Build DuckDB-compatible mysql connection string."""
    host = mc.get_container_host_ip()
    port = mc.get_exposed_port(3306)
    return f"mysql:host={host} port={port} user=root password={mc.root_password} database={db_name}"


# ---------------------------------------------------------------------------
# Catalog fixtures (per-backend)
# ---------------------------------------------------------------------------


@pytest.fixture()
def pg_catalog(postgres: PostgresContainer, tmp_path: os.PathLike[str]) -> Generator[Catalog, None, None]:
    """Per-test catalog: PostgreSQL metadata + local data files."""
    db_name = f"test_{uuid.uuid4().hex[:12]}"
    _create_pg_database(postgres, db_name)

    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)

    cat = Catalog("integ", _pg_connection_string(postgres, db_name), data_path=data_dir)
    yield cat
    cat.close()


@pytest.fixture()
def s3_catalog(postgres: PostgresContainer, minio: MinioContainer) -> Generator[Catalog, None, None]:
    """Per-test catalog: PostgreSQL metadata + MinIO S3 data files."""
    db_name = f"test_{uuid.uuid4().hex[:12]}"
    _create_pg_database(postgres, db_name)

    test_prefix = uuid.uuid4().hex[:8]
    data_path = f"s3://{MINIO_BUCKET}/{test_prefix}/"

    cat = Catalog(
        "s3cat",
        _pg_connection_string(postgres, db_name),
        data_path=data_path,
        properties=_s3_properties(minio),
    )
    yield cat
    cat.close()


@pytest.fixture()
def mysql_catalog(mysql: MySqlContainer, tmp_path: os.PathLike[str]) -> Generator[Catalog, None, None]:
    """Per-test catalog: MySQL metadata + local data files."""
    db_name = f"test_{uuid.uuid4().hex[:12]}"
    _create_mysql_database(mysql, db_name)

    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)

    cat = Catalog("mysql_cat", _mysql_connection_string(mysql, db_name), data_path=data_dir)
    yield cat
    cat.close()


@pytest.fixture()
def sqlite_catalog(tmp_path: os.PathLike[str]) -> Generator[Catalog, None, None]:
    """Per-test catalog: SQLite metadata + local data files."""
    meta_db = str(tmp_path / "meta.sqlite")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)

    cat = Catalog("sqlite_cat", f"sqlite:{meta_db}", data_path=data_dir)
    yield cat
    cat.close()


@pytest.fixture()
def duckdb_catalog(tmp_path: os.PathLike[str]) -> Generator[Catalog, None, None]:
    """Per-test catalog: DuckDB local file metadata + local data files."""
    meta_db = str(tmp_path / "meta.duckdb")
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)

    cat = Catalog("duckdb_cat", meta_db, data_path=data_dir)
    yield cat
    cat.close()


# ---------------------------------------------------------------------------
# Parameterized catalog fixture — runs each test against all backends
# ---------------------------------------------------------------------------

# Lazy container cache: containers are started once per process and reused.
_container_cache: dict[str, object] = {}


def _get_postgres() -> PostgresContainer:
    if "postgres" not in _container_cache:
        pg = PostgresContainer("postgres:17", driver=None)
        pg.start()
        _wait_for_postgres(pg)
        _container_cache["postgres"] = pg
    return _container_cache["postgres"]  # type: ignore[return-value]


def _get_mysql() -> MySqlContainer:
    if "mysql" not in _container_cache:
        mc = MySqlContainer("mysql:8.0")
        mc.start()
        _wait_for_mysql(mc)
        _container_cache["mysql"] = mc
    return _container_cache["mysql"]  # type: ignore[return-value]


def _stop_containers() -> None:
    for container in _container_cache.values():
        try:
            container.stop()  # type: ignore[union-attr]
        except Exception:
            pass
    _container_cache.clear()


import atexit  # noqa: E402

atexit.register(_stop_containers)


@pytest.fixture(params=["duckdb", "postgres", "sqlite", "mysql"])
def catalog(request: pytest.FixtureRequest, tmp_path: os.PathLike[str]) -> Generator[Catalog, None, None]:
    """Parameterized catalog fixture — runs each test against all backends."""
    backend = request.param
    data_dir = str(tmp_path / "data")
    os.makedirs(data_dir, exist_ok=True)

    if backend == "duckdb":
        meta_db = str(tmp_path / "meta.duckdb")
        cat = Catalog("test_cat", meta_db, data_path=data_dir)
    elif backend == "postgres":
        pg = _get_postgres()
        db_name = f"test_{uuid.uuid4().hex[:12]}"
        _create_pg_database(pg, db_name)
        cat = Catalog("test_cat", _pg_connection_string(pg, db_name), data_path=data_dir)
    elif backend == "sqlite":
        meta_db = str(tmp_path / "meta.sqlite")
        cat = Catalog("test_cat", f"sqlite:{meta_db}", data_path=data_dir)
    elif backend == "mysql":
        mc = _get_mysql()
        db_name = f"test_{uuid.uuid4().hex[:12]}"
        _create_mysql_database(mc, db_name)
        cat = Catalog("test_cat", _mysql_connection_string(mc, db_name), data_path=data_dir)
    else:
        raise ValueError(f"Unknown backend: {backend}")

    yield cat
    cat.close()


# ---------------------------------------------------------------------------
# Shared schema fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def simple_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType()),
        NestedField(field_id=3, name="value", field_type=DoubleType()),
    )
