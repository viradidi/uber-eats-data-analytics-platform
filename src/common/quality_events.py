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

def quarantine_rejected_rows(spark, rejected_df, dataset_name, reason, run_id=None):
    """Append rejected records as JSON to a shared Delta quarantine table.

    The write is strict: callers must fail if quarantine persistence fails.
    """
    from pyspark.sql.functions import col, lit, struct, to_json

    if not rejected_df.columns:
        return 0
    payload = rejected_df.select(
        to_json(struct(*[col(name) for name in rejected_df.columns])).alias("record_json")
    )
    rejected_count = payload.count()
    if rejected_count == 0:
        return 0

    quarantine_run_id = run_id or str(uuid4())
    (
        payload
        .withColumn("run_id", lit(quarantine_run_id))
        .withColumn("dataset_name", lit(dataset_name))
        .withColumn("rejection_reason", lit(str(reason)[:1000]))
        .withColumn("quarantined_at_utc", lit(datetime.now(timezone.utc).isoformat()))
        .select("run_id", "dataset_name", "rejection_reason", "quarantined_at_utc", "record_json")
        .write.format("delta")
        .mode("append")
        .saveAsTable("ubereats_databricks1.silver.pipeline_rejected_records")
    )
    print(f"{dataset_name}.quarantined_rows={rejected_count}; run_id={quarantine_run_id}")
    return rejected_count
