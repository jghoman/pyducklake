"""Opt-in, redacted DuckDB profiles for table append operations."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

import duckdb

_LOG = logging.getLogger(__name__)

_PROPERTY = "pyducklake_profile_append"
_PROFILING_SETTINGS = frozenset(
    {
        "enable_profiling",
        "profiling_coverage",
        "profiling_mode",
        "profiling_output",
        "profile_output",
        "custom_profiling_settings",
    }
)
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})

_ROOT_FIELDS = frozenset(
    {
        "total_memory_allocated",
        "total_bytes_written",
        "total_bytes_read",
        "system_peak_temp_dir_size",
        "system_peak_buffer_memory",
        "rows_returned",
        "result_set_size",
        "latency",
        "cpu_time",
        "blocked_thread_time",
        "cumulative_cardinality",
        "cumulative_rows_scanned",
        "children",
    }
)
_OPERATOR_FIELDS = frozenset(
    {
        "operator_name",
        "operator_type",
        "operator_timing",
        "operator_rows_scanned",
        "operator_cardinality",
        "result_set_size",
        "system_peak_temp_dir_size",
        "system_peak_buffer_memory",
        "cpu_time",
        "cumulative_cardinality",
        "cumulative_rows_scanned",
        "children",
    }
)


def split_append_profile_properties(properties: dict[str, str] | None) -> tuple[dict[str, str], AppendProfiler]:
    """Remove PyDuckLake profiling properties from DuckDB connection properties."""
    remaining = dict(properties or {})
    profile_properties = [key for key in remaining if key.lower() == _PROPERTY]
    if len(profile_properties) > 1:
        raise ValueError(f"{_PROPERTY} may be specified only once")
    value = remaining.pop(profile_properties[0], "false") if profile_properties else "false"
    enabled = _parse_bool(value)

    if enabled:
        conflicting = sorted(key for key in remaining if key.lower() in _PROFILING_SETTINGS)
        if conflicting:
            names = ", ".join(conflicting)
            raise ValueError(f"{_PROPERTY} cannot be combined with DuckDB profiling settings: {names}")

    return remaining, AppendProfiler(enabled=enabled)


def _parse_bool(value: str) -> bool:
    normalized = value.lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"{_PROPERTY} must be one of true/false, got {value!r}")


class AppendProfiler:
    """Capture one append profile without changing write success or failure."""

    def __init__(self, *, enabled: bool) -> None:
        self._enabled = enabled

    def start(self, connection: duckdb.DuckDBPyConnection) -> _ProfileSession | None:
        """Enable in-memory profiling for the immediately following append query."""
        if not self._enabled:
            return None

        session = _current_profile_session(connection)
        if session is None:
            _LOG.warning("pyducklake_append_profile_start_failed", exc_info=True)
            return None
        if session.enabled is not None:
            _LOG.warning("pyducklake_append_profile_skipped_existing_profile")
            return None

        profiling_enabled = False
        try:
            connection.execute("SET enable_profiling = 'no_output'")
            profiling_enabled = True
            connection.execute("SET profiling_coverage = 'ALL'")
        except Exception:
            if profiling_enabled:
                self.stop(connection, session=session)
            _LOG.warning("pyducklake_append_profile_start_failed", exc_info=True)
            return None
        return session

    def capture(self, connection: duckdb.DuckDBPyConnection, *, catalog: str, table: str) -> None:
        """Log the current profile without masking a successful write."""
        try:
            raw_profile = connection.get_profiling_information("json")
            profile = _parse_profile(raw_profile)
            if profile is None:
                _LOG.warning("pyducklake_append_profile_unavailable")
            else:
                event = {
                    "catalog": catalog,
                    "event": "pyducklake_append_profile",
                    "profile": _redact_profile(profile, fields=_ROOT_FIELDS),
                    "table": table,
                }
                _LOG.info("pyducklake_append_profile %s", json.dumps(event, sort_keys=True, separators=(",", ":")))
        except Exception:
            _LOG.warning("pyducklake_append_profile_capture_failed", exc_info=True)

    def stop(self, connection: duckdb.DuckDBPyConnection, *, session: _ProfileSession) -> None:
        """Best-effort cleanup after a profile attempt."""
        try:
            connection.execute("PRAGMA disable_profiling")
        except Exception:
            _LOG.warning("pyducklake_append_profile_disable_failed", exc_info=True)
        try:
            connection.execute(f"SET profiling_coverage = '{session.coverage}'")
        except Exception:
            _LOG.warning("pyducklake_append_profile_coverage_reset_failed", exc_info=True)


@dataclass(frozen=True)
class _ProfileSession:
    enabled: str | None
    coverage: str


def _current_profile_session(connection: duckdb.DuckDBPyConnection) -> _ProfileSession | None:
    try:
        row = connection.execute(
            "SELECT current_setting('enable_profiling'), current_setting('profiling_coverage')"
        ).fetchone()
    except Exception:
        return None
    if row is None or len(row) != 2:
        return None
    enabled, coverage = row
    if enabled is not None and not isinstance(enabled, str):
        return None
    if not isinstance(coverage, str):
        return None
    return _ProfileSession(enabled=enabled, coverage=coverage)


def _parse_profile(raw_profile: Any) -> Mapping[str, Any] | None:
    if isinstance(raw_profile, str):
        parsed = json.loads(raw_profile)
    else:
        parsed = raw_profile
    if isinstance(parsed, Mapping):
        return cast(Mapping[str, Any], parsed)
    return None


def _redact_profile(profile: Mapping[str, Any], *, fields: frozenset[str]) -> dict[str, Any]:
    """Keep fixed timing/memory fields while dropping SQL and operator metadata."""
    redacted: dict[str, Any] = {}
    for field in fields:
        value = profile.get(field)
        if field == "children":
            redacted[field] = _redact_children(value)
        elif value is None or isinstance(value, str | int | float | bool):
            redacted[field] = value
    return redacted


def _redact_children(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    children: list[dict[str, Any]] = []
    items = cast(list[object], value)
    for child in items:
        if isinstance(child, Mapping):
            children.append(_redact_profile(cast(Mapping[str, Any], child), fields=_OPERATOR_FIELDS))
    return children
