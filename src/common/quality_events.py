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


def reconcile_merge_target(
    spark,
    source_df,
    target_table: str,
    dataset_name: str,
    key_columns: list[str],
    compare_columns: list[str],
) -> dict:
    """Reconcile prepared merge rows against the published Delta target.

    Verifies source-key coverage and null-safe equality for selected business
    fields. Persists an audit row before raising when reconciliation fails.
    """
    from functools import reduce
    from pyspark.sql.functions import col, lit

    if not key_columns:
        raise ValueError("key_columns must contain at least one business key")
    if not set(key_columns + compare_columns).issubset(set(source_df.columns)):
        missing = sorted(set(key_columns + compare_columns) - set(source_df.columns))
        raise ValueError(f"{dataset_name} reconciliation source is missing columns: {missing}")

    run_id = str(uuid4())
    target_df = spark.table(target_table)
    missing_target_keys = (
        source_df.select(*key_columns)
        .join(target_df.select(*key_columns), on=key_columns, how="left_anti")
        .count()
    )

    source_alias = source_df.alias("s")
    target_alias = target_df.alias("t")
    matched = source_alias.join(target_alias, on=key_columns, how="inner")
    mismatch_condition = reduce(
        lambda left, right: left | right,
        [~col(f"s.{name}").eqNullSafe(col(f"t.{name}")) for name in compare_columns],
        lit(False),
    )
    mismatched_rows = matched.filter(mismatch_condition).count() if compare_columns else 0
    source_rows = source_df.count()
    status = "SUCCEEDED" if missing_target_keys == 0 and mismatched_rows == 0 else "FAILED_RECONCILIATION"
    record = [{
        "run_id": run_id,
        "dataset_name": dataset_name,
        "target_table": target_table,
        "source_rows": int(source_rows),
        "missing_target_keys": int(missing_target_keys),
        "mismatched_rows": int(mismatched_rows),
        "status": status,
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
    }]
    (
        spark.createDataFrame(record)
        .write.format("delta")
        .mode("append")
        .saveAsTable("ubereats_databricks1.silver.pipeline_reconciliation_results")
    )
    print(
        f"{dataset_name}.reconciliation.status={status}; source_rows={source_rows}; "
        f"missing_target_keys={missing_target_keys}; mismatched_rows={mismatched_rows}; run_id={run_id}"
    )
    if status != "SUCCEEDED":
        error = RuntimeError(
            f"{dataset_name} reconciliation failed: {missing_target_keys} missing target keys, "
            f"{mismatched_rows} rows with field mismatches"
        )
        log_pipeline_failure(spark, dataset_name, "silver_reconciliation", error)
        raise error
    return record[0]
