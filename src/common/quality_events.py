"""Best-effort Delta event logging for notebook quality and pipeline failures.

Logging errors are intentionally swallowed so observability never masks the
original validation or publishing failure. The target table must be writable
for durable events to be retained.
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4


def log_pipeline_failure(spark, dataset_name: str, stage: str, error: Exception) -> None:
    """Append a structured failure event without masking the original error."""
    record = [{
        "run_id": str(uuid4()),
        "dataset_name": dataset_name,
        "stage": stage,
        "error_type": type(error).__name__,
        "error_message": str(error)[:2000],
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
    }]
    try:
        (
            spark.createDataFrame(record)
            .write.format("delta")
            .mode("append")
            .saveAsTable("ubereats_databricks1.silver.pipeline_failure_events")
        )
    except Exception as logging_error:
        print(
            f"{dataset_name}.failure_event_logging_error="
            f"{type(logging_error).__name__}: {str(logging_error)[:500]}"
        )
