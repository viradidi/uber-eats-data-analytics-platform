"""Retention planning and guarded cleanup SQL for Gold publication versions."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

from src.gold.publication import (
    GOLD_OUTPUT_NAMES,
    PUBLICATION_KEY,
    qualified_name,
)


SUCCESS_EVENT_STATUSES = {
    "PUBLISHED",
    "PUBLISHED_WITH_WARNINGS",
    "LEGACY_BASELINE_IMPORTED",
}
BASELINE_EVENT_STATUS = "LEGACY_BASELINE_IMPORTED"
IN_PROGRESS_EVENT_STATUSES = {
    "STAGING_STARTED",
    "SNAPSHOTS_STAGED",
}
FAILED_EVENT_STATUSES = {"FAILED_BEFORE_COMMIT"}
KNOWN_EVENT_STATUSES = (
    SUCCESS_EVENT_STATUSES
    | IN_PROGRESS_EVENT_STATUSES
    | FAILED_EVENT_STATUSES
)
TERMINAL_EVENT_STATUSES = SUCCESS_EVENT_STATUSES | FAILED_EVENT_STATUSES
MIN_RETENTION_DAYS = 7


def _read_value(row: Any, field: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(field)
    if hasattr(row, field):
        return getattr(row, field)
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True).get(field)
    return None


def _as_utc(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            value = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        # Spark commonly returns UTC TIMESTAMP values as naive Python datetimes.
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def build_retention_plan(
    publication_history: Iterable[Any],
    version_publication_ids: Iterable[str],
    *,
    active_publication_id: str | None,
    now_utc: datetime,
    retention_days: int = 30,
    keep_successful_publications: int = 10,
) -> dict[str, Any]:
    """Plan deletion only for known, terminal, expired, inactive publications.

    Fail-closed rules:
    - keep the active publication and the legacy baseline;
    - keep the newest N successful publications;
    - keep recent publications and all non-terminal/in-flight runs;
    - keep IDs found in version tables but missing from publication history;
    - keep publications with unknown event statuses or missing timestamps.
    """
    if not isinstance(retention_days, int) or retention_days < MIN_RETENTION_DAYS:
        raise ValueError(f"retention_days must be at least {MIN_RETENTION_DAYS}.")
    if not isinstance(keep_successful_publications, int) or keep_successful_publications < 1:
        raise ValueError("keep_successful_publications must be at least 1.")
    current_time = _as_utc(now_utc)
    if current_time is None:
        raise ValueError("now_utc must be a valid datetime.")
    cutoff = current_time - timedelta(days=retention_days)

    inventory = {
        str(value).strip()
        for value in version_publication_ids
        if value is not None and str(value).strip()
    }
    events_by_publication: dict[str, list[tuple[datetime | None, str]]] = {}
    for event in publication_history:
        publication_id = _read_value(event, "publication_id")
        if publication_id is None or not str(publication_id).strip():
            continue
        publication_id = str(publication_id).strip()
        event_status = str(_read_value(event, "event_status") or "").strip().upper()
        recorded_at = _as_utc(_read_value(event, "recorded_at_utc"))
        events_by_publication.setdefault(publication_id, []).append(
            (recorded_at, event_status)
        )

    successful_times: dict[str, datetime] = {}
    legacy_baselines: set[str] = set()
    for publication_id, events in events_by_publication.items():
        successful = [
            event_time
            for event_time, status in events
            if status in SUCCESS_EVENT_STATUSES and event_time is not None
        ]
        if successful:
            successful_times[publication_id] = max(successful)
        if any(status == BASELINE_EVENT_STATUS for _, status in events):
            legacy_baselines.add(publication_id)

    newest_successful = {
        publication_id
        for publication_id, _ in sorted(
            successful_times.items(),
            key=lambda item: (item[1], item[0]),
            reverse=True,
        )[:keep_successful_publications]
    }

    candidates: list[str] = []
    preserved: dict[str, str] = {}
    untracked: list[str] = []
    unknown_status: list[str] = []
    in_flight: list[str] = []
    missing_timestamps: list[str] = []
    recent: list[str] = []

    for publication_id in sorted(inventory):
        events = events_by_publication.get(publication_id, [])
        if not events:
            untracked.append(publication_id)
            preserved[publication_id] = "NO_PUBLICATION_HISTORY"
            continue
        if publication_id == active_publication_id:
            preserved[publication_id] = "ACTIVE_PUBLICATION"
            continue
        if publication_id in legacy_baselines:
            preserved[publication_id] = "LEGACY_BASELINE"
            continue
        if publication_id in newest_successful:
            preserved[publication_id] = "RECENT_SUCCESSFUL_RELEASE"
            continue
        if any(status not in KNOWN_EVENT_STATUSES for _, status in events):
            unknown_status.append(publication_id)
            preserved[publication_id] = "UNKNOWN_EVENT_STATUS"
            continue

        ordered_events = sorted(
            events,
            key=lambda event: (
                event[0] or datetime.min.replace(tzinfo=timezone.utc),
                event[1] in TERMINAL_EVENT_STATUSES,
            ),
        )
        latest_time, latest_status = ordered_events[-1]
        if latest_status in IN_PROGRESS_EVENT_STATUSES:
            in_flight.append(publication_id)
            preserved[publication_id] = "NON_TERMINAL_PUBLICATION"
            continue
        if latest_status not in TERMINAL_EVENT_STATUSES:
            in_flight.append(publication_id)
            preserved[publication_id] = "NON_TERMINAL_PUBLICATION"
            continue
        if any(event_time is None for event_time, _ in events):
            missing_timestamps.append(publication_id)
            preserved[publication_id] = "MISSING_EVENT_TIMESTAMP"
            continue
        if latest_time is None:
            missing_timestamps.append(publication_id)
            preserved[publication_id] = "MISSING_EVENT_TIMESTAMP"
            continue
        if latest_time >= cutoff:
            recent.append(publication_id)
            preserved[publication_id] = "WITHIN_RETENTION_WINDOW"
            continue
        candidates.append(publication_id)

    return {
        "active_publication_id": active_publication_id,
        "retention_days": retention_days,
        "keep_successful_publications": keep_successful_publications,
        "cutoff_utc": cutoff.isoformat(),
        "candidate_publication_ids": candidates,
        "preserved": preserved,
        "untracked_publication_ids": untracked,
        "unknown_status_publication_ids": unknown_status,
        "in_flight_publication_ids": in_flight,
        "missing_timestamp_publication_ids": missing_timestamps,
        "recent_publication_ids": recent,
        "newest_successful_publication_ids": sorted(newest_successful),
        "inventory_publication_count": len(inventory),
    }


def build_guarded_delete_sql(
    catalog: str,
    schema: str,
    output_name: str,
    publication_ids: Iterable[str],
) -> str | None:
    """Build a DELETE guarded against removing the active publication pointer."""
    if output_name not in GOLD_OUTPUT_NAMES:
        raise ValueError(f"Unsupported Gold output: {output_name!r}")
    ids = sorted({str(value) for value in publication_ids if value is not None})
    if not ids:
        return None
    for publication_id in ids:
        if not publication_id:
            raise ValueError("publication IDs must not be empty.")
    literals = ", ".join(
        "'" + publication_id.replace("'", "''") + "'"
        for publication_id in ids
    )
    target = qualified_name(catalog, schema, f"{output_name}__versions")
    state = qualified_name(catalog, schema, "gold_publication_state")
    return f"""DELETE FROM {target} AS version
WHERE version._publication_id IN ({literals})
  AND NOT EXISTS (
      SELECT 1
      FROM {state} AS active
      WHERE active.publication_key = '{PUBLICATION_KEY}'
        AND active.active_publication_id = version._publication_id
  )"""
