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

def reconcile_full_snapshot(
    spark,
    source_df,
    target_table: str,
    dataset_name: str,
    key_columns: list[str],
    *,
    source_is_full_snapshot: bool,
    aggregate_specs: dict[str, str] | None = None,
    numeric_tolerance: float = 1e-9,
) -> dict:
    """Audit a confirmed full snapshot without deleting or modifying target data.

    Explicit full-snapshot confirmation is required because target-only keys are
    expected for incremental batches. Records are appended to a separate detail
    table so the existing reconciliation table schema remains unchanged.
    """
    from math import isclose

    if source_is_full_snapshot is not True:
        raise ValueError(
            "Pass source_is_full_snapshot=True only for a confirmed full snapshot; "
            "incremental batches should use reconcile_merge_target()"
        )
    if not key_columns:
        raise ValueError("key_columns must contain at least one business key")

    aggregate_specs = aggregate_specs or {}
    allowed_aggregates = {"sum", "min", "max", "avg", "count"}
    invalid_specs = {
        name: operation for name, operation in aggregate_specs.items()
        if operation not in allowed_aggregates
    }
    if invalid_specs:
        raise ValueError(f"Unsupported aggregate specifications: {invalid_specs}")

    from pyspark.sql import functions as F

    required_columns = set(key_columns) | set(aggregate_specs)
    missing_source = sorted(required_columns - set(source_df.columns))
    if missing_source:
        raise ValueError(f"{dataset_name} snapshot source is missing columns: {missing_source}")

    run_id = str(uuid4())
    target_df = spark.table(target_table)
    missing_target = sorted(required_columns - set(target_df.columns))
    if missing_target:
        raise ValueError(f"{dataset_name} snapshot target is missing columns: {missing_target}")

    def duplicate_key_groups(frame):
        return (
            frame.groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

    source_duplicate_groups = duplicate_key_groups(source_df)
    target_duplicate_groups = duplicate_key_groups(target_df)
    if source_duplicate_groups or target_duplicate_groups:
        error = ValueError(
            f"{dataset_name} snapshot reconciliation requires unique keys; "
            f"source_duplicate_groups={source_duplicate_groups}, "
            f"target_duplicate_groups={target_duplicate_groups}"
        )
        log_pipeline_failure(spark, dataset_name, "snapshot_reconciliation_precondition", error)
        raise error

    source_keys = source_df.select(*key_columns)
    target_keys = target_df.select(*key_columns)
    source_only_keys = source_keys.join(target_keys, on=key_columns, how="left_anti").count()
    target_only_keys = target_keys.join(source_keys, on=key_columns, how="left_anti").count()

    def aggregate_values(frame):
        expressions = [F.count(F.lit(1)).alias("__row_count")]
        for column_name, operation in aggregate_specs.items():
            expressions.append(
                getattr(F, operation)(F.col(column_name)).alias(f"__aggregate_{column_name}")
            )
        return frame.agg(*expressions).first().asDict()

    source_values = aggregate_values(source_df)
    target_values = aggregate_values(target_df)
    checks = [
        ("key_coverage", "source_only_key_count", source_only_keys, 0),
        ("key_coverage", "target_only_key_count", 0, target_only_keys),
        ("row_count", "row_count", source_values["__row_count"], target_values["__row_count"]),
    ]
    for column_name, operation in aggregate_specs.items():
        checks.append((
            "business_aggregate",
            f"{operation}:{column_name}",
            source_values[f"__aggregate_{column_name}"],
            target_values[f"__aggregate_{column_name}"],
        ))

    def values_match(expected, actual):
        if expected is None or actual is None:
            return expected is actual
        if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
            return isclose(
                float(expected), float(actual),
                rel_tol=numeric_tolerance, abs_tol=numeric_tolerance,
            )
        try:
            from decimal import Decimal
            return Decimal(str(expected)) == Decimal(str(actual))
        except Exception:
            return expected == actual

    failed_checks = []
    records = []
    recorded_at = datetime.now(timezone.utc).isoformat()
    for check_type, metric_name, expected, actual in checks:
        passed = values_match(expected, actual)
        if not passed:
            failed_checks.append(metric_name)
        difference = None
        if expected is not None and actual is not None:
            try:
                difference = str(actual - expected)
            except TypeError:
                difference = "not_numeric"
        records.append({
            "run_id": run_id,
            "dataset_name": dataset_name,
            "target_table": target_table,
            "check_type": check_type,
            "metric_name": metric_name,
            "source_value": None if expected is None else str(expected),
            "target_value": None if actual is None else str(actual),
            "difference": difference,
            "status": "SUCCEEDED" if passed else "FAILED_RECONCILIATION",
            "recorded_at_utc": recorded_at,
        })

    from pyspark.sql.types import StringType, StructField, StructType
    detail_schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("dataset_name", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("check_type", StringType(), False),
        StructField("metric_name", StringType(), False),
        StructField("source_value", StringType(), True),
        StructField("target_value", StringType(), True),
        StructField("difference", StringType(), True),
        StructField("status", StringType(), False),
        StructField("recorded_at_utc", StringType(), False),
    ])
    (
        spark.createDataFrame(records, schema=detail_schema)
        .write.format("delta")
        .mode("append")
        .saveAsTable("ubereats_databricks1.silver.pipeline_reconciliation_details")
    )

    status = "SUCCEEDED" if not failed_checks else "FAILED_RECONCILIATION"
    print(
        f"{dataset_name}.full_snapshot_reconciliation.status={status}; "
        f"source_only_keys={source_only_keys}; target_only_keys={target_only_keys}; "
        f"failed_checks={failed_checks}; run_id={run_id}"
    )
    if failed_checks:
        error = RuntimeError(
            f"{dataset_name} full-snapshot reconciliation failed checks: "
            + ", ".join(failed_checks)
        )
        log_pipeline_failure(spark, dataset_name, "full_snapshot_reconciliation", error)
        raise error

    return {
        "run_id": run_id,
        "dataset_name": dataset_name,
        "target_table": target_table,
        "source_only_keys": source_only_keys,
        "target_only_keys": target_only_keys,
        "status": status,
        "failed_checks": failed_checks,
    }
