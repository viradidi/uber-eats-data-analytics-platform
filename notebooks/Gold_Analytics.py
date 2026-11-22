# Databricks notebook source
"""Publish the Uber Eats Gold analytics layer from validated Silver tables."""

import sys

dbutils.widgets.text("project_root", "/Workspace/Shared/UberEats", "Workspace package root")
PROJECT_ROOT = dbutils.widgets.get("project_root").strip().rstrip("/")
if PROJECT_ROOT and PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from datetime import datetime, timezone
import json
import re
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.gold.transformations import build_gold_outputs
from src.gold.quality import (
    build_gold_quality_observations,
    evaluate_quality_anomalies,
    quality_metrics_schema,
)
from src.gold.publication import (
    GOLD_OUTPUT_NAMES,
    active_publication_id,
    build_activation_merge_sql,
    build_publication_view_sql,
    history_table_name,
    publication_history_schema,
    publication_state_schema,
    qualified_name,
    with_publication_id,
)


# COMMAND ----------

dbutils.widgets.text("catalog", "ubereats_databricks1", "Unity Catalog catalog")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("gold_schema", "gold", "Gold schema")
dbutils.widgets.text("pipeline_run_id", "manual", "Parent ADF pipeline run ID")
dbutils.widgets.text("quality_min_history_runs", "5", "Prior runs required for anomaly detection")
dbutils.widgets.text("quality_z_score_threshold", "3.0", "Anomaly standard-deviation threshold")
dbutils.widgets.text("quality_relative_change_threshold", "0.50", "Minimum relative change for anomaly")
dbutils.widgets.text("quality_absolute_floor", "0.05", "Minimum absolute deviation tolerance")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id") or "manual"
quality_min_history_runs = int(dbutils.widgets.get("quality_min_history_runs"))
quality_z_score_threshold = float(dbutils.widgets.get("quality_z_score_threshold"))
quality_relative_change_threshold = float(dbutils.widgets.get("quality_relative_change_threshold"))
quality_absolute_floor = float(dbutils.widgets.get("quality_absolute_floor"))

if quality_min_history_runs < 1:
    raise ValueError("quality_min_history_runs must be at least 1.")
if quality_z_score_threshold <= 0 or quality_relative_change_threshold <= 0:
    raise ValueError("Quality anomaly thresholds must be greater than zero.")
if quality_absolute_floor < 0:
    raise ValueError("quality_absolute_floor must not be negative.")

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
for label, value in (("catalog", catalog), ("silver_schema", silver_schema), ("gold_schema", gold_schema)):
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe {label} identifier: {value!r}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

run_id = str(uuid4())
started_at = datetime.now(timezone.utc).isoformat()
audit_table = f"{catalog}.{gold_schema}.gold_run_audit"
audit_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_run_id", StringType(), False),
    StructField("status", StringType(), False),
    StructField("outputs_json", StringType(), False),
    StructField("started_at_utc", StringType(), False),
    StructField("finished_at_utc", StringType(), False),
    StructField("error_message", StringType(), True),
])


def write_audit(status, output_counts, error_message=None):
    record = [{
        "run_id": run_id,
        "pipeline_run_id": pipeline_run_id,
        "status": status,
        "outputs_json": json.dumps(output_counts, sort_keys=True),
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "error_message": error_message[:2000] if error_message else None,
    }]
    (
        spark.createDataFrame(record, schema=audit_schema)
        .write.format("delta")
        .mode("append")
        .saveAsTable(audit_table)
    )


def _raw_table_name(table_name):
    return f"{catalog}.{gold_schema}.{table_name}"


def _table_exists(table_name):
    return spark.catalog.tableExists(_raw_table_name(table_name))


def _table_type(table_name):
    if not _table_exists(table_name):
        return None
    return spark.catalog.getTable(_raw_table_name(table_name)).tableType.upper()


def _ensure_empty_delta_table(table_name, schema):
    if not spark.catalog.tableExists(table_name):
        spark.createDataFrame([], schema=schema).write.format("delta").mode("overwrite").saveAsTable(table_name)


def _append_publication_event(
    publication_id,
    event_status,
    previous_publication_id=None,
    output_counts=None,
    error_message=None,
):
    event_table = _raw_table_name("gold_publication_history")
    row = [(
        str(uuid4()),
        publication_id,
        pipeline_run_id,
        event_status,
        datetime.now(timezone.utc).replace(tzinfo=None),
        previous_publication_id,
        json.dumps(output_counts or {}, sort_keys=True),
        error_message[:2000] if error_message else None,
    )]
    (
        spark.createDataFrame(row, schema=publication_history_schema())
        .write.format("delta")
        .mode("append")
        .saveAsTable(event_table)
    )


def _ensure_publication_views(gold_frames):
    """Bootstrap stable views while preserving existing physical Gold tables."""
    state_table = _raw_table_name("gold_publication_state")
    event_table = _raw_table_name("gold_publication_history")
    _ensure_empty_delta_table(state_table, publication_state_schema())
    _ensure_empty_delta_table(event_table, publication_history_schema())

    active_id = active_publication_id(spark, catalog, gold_schema)
    for output_name in GOLD_OUTPUT_NAMES:
        version_table = _raw_table_name(history_table_name(output_name))
        if not spark.catalog.tableExists(version_table):
            if active_id is not None:
                raise RuntimeError(
                    f"GOLD_PUBLICATION_VERSION_MISSING: active publication {active_id} "
                    f"has no backing table {version_table}."
                )
            empty_version = (
                gold_frames[output_name]
                .withColumn("_publication_id", F.lit(None).cast("string"))
                .limit(0)
            )
            empty_version.write.format("delta").mode("overwrite").saveAsTable(version_table)
        else:
            expected_columns = list(gold_frames[output_name].columns) + ["_publication_id"]
            actual_columns = spark.table(version_table).columns
            if actual_columns != expected_columns:
                raise RuntimeError(
                    f"{version_table} schema mismatch; expected {expected_columns}, found {actual_columns}"
                )

    existing_types = {name: _table_type(name) for name in GOLD_OUTPUT_NAMES}
    existing_names = {name for name, object_type in existing_types.items() if object_type}
    physical_names = {
        name for name, object_type in existing_types.items()
        if object_type and object_type != "VIEW"
    }
    view_names = {
        name for name, object_type in existing_types.items()
        if object_type == "VIEW"
    }

    # First-time migration: copy the complete previous Gold set into one
    # baseline version and commit its pointer before replacing any old table.
    if active_id is None and physical_names:
        if physical_names != set(GOLD_OUTPUT_NAMES) or view_names:
            raise RuntimeError(
                "GOLD_PUBLICATION_BOOTSTRAP_INCOMPLETE: expected either all five "
                "legacy Gold tables or none; no legacy object was renamed."
            )
        bootstrap_id = f"bootstrap-{uuid4().hex}"
        bootstrap_counts = {}
        for output_name in GOLD_OUTPUT_NAMES:
            old_name = _raw_table_name(output_name)
            old_frame = spark.table(old_name)
            if old_frame.columns != gold_frames[output_name].columns:
                raise RuntimeError(
                    f"GOLD_PUBLICATION_SCHEMA_MISMATCH: legacy {old_name} columns "
                    "do not match the validated Gold output contract."
                )
            expected_count = int(old_frame.count())
            version_name = _raw_table_name(history_table_name(output_name))
            (
                with_publication_id(old_frame, bootstrap_id)
                .write.format("delta")
                .mode("append")
                .saveAsTable(version_name)
            )
            actual_count = (
                spark.table(version_name)
                .filter(F.col("_publication_id") == F.lit(bootstrap_id))
                .count()
            )
            if int(actual_count) != expected_count:
                raise RuntimeError(
                    f"GOLD_PUBLICATION_BOOTSTRAP_COUNT_MISMATCH for {output_name}: "
                    f"expected {expected_count}, found {actual_count}"
                )
            bootstrap_counts[output_name] = expected_count

        spark.sql(build_activation_merge_sql(
            catalog,
            gold_schema,
            publication_id=bootstrap_id,
            expected_previous_publication_id=None,
            pipeline_run_id=pipeline_run_id,
        ))
        active_id = active_publication_id(spark, catalog, gold_schema)
        if active_id != bootstrap_id:
            raise RuntimeError("GOLD_PUBLICATION_BOOTSTRAP_CONFLICT: legacy baseline pointer was not committed.")
    elif active_id is None and view_names:
        raise RuntimeError(
            "GOLD_PUBLICATION_STATE_MISSING: stable Gold views exist without an active publication pointer."
        )

    # Reconcile the bootstrap event if a previous process committed the
    # baseline pointer but stopped before writing its lifecycle record.
    if active_id and active_id.startswith("bootstrap-"):
        bootstrap_event_exists = (
            spark.table(event_table)
            .filter(
                (F.col("publication_id") == F.lit(active_id))
                & (F.col("event_status") == F.lit("LEGACY_BASELINE_IMPORTED"))
            )
            .limit(1)
            .count()
        )
        if not bootstrap_event_exists:
            bootstrap_counts = {
                output_name: int(
                    spark.table(_raw_table_name(history_table_name(output_name)))
                    .filter(F.col("_publication_id") == F.lit(active_id))
                    .count()
                )
                for output_name in GOLD_OUTPUT_NAMES
            }
            _append_publication_event(
                active_id,
                "LEGACY_BASELINE_IMPORTED",
                previous_publication_id=None,
                output_counts=bootstrap_counts,
            )

    # If migration was interrupted after the baseline pointer was committed,
    # complete the remaining table-to-view replacements on the next run.
    for output_name in GOLD_OUTPUT_NAMES:
        version_table = _raw_table_name(history_table_name(output_name))
        stable_table = _raw_table_name(output_name)
        object_type = _table_type(output_name)

        if object_type and object_type != "VIEW":
            if active_id is None:
                raise RuntimeError(
                    f"GOLD_PUBLICATION_NO_ACTIVE_POINTER: cannot replace {stable_table} safely."
                )
            legacy_name = f"{output_name}__legacy_{uuid4().hex[:8]}"
            spark.sql(
                f"ALTER TABLE {qualified_name(catalog, gold_schema, output_name)} "
                f"RENAME TO {qualified_name(catalog, gold_schema, legacy_name)}"
            )

        if active_id is not None and not spark.catalog.tableExists(version_table):
            raise RuntimeError(
                f"GOLD_PUBLICATION_VERSION_MISSING: active publication {active_id} "
                f"has no backing table {version_table}."
            )

        should_replace_view = object_type != "VIEW"
        if object_type == "VIEW":
            current_columns = spark.table(stable_table).columns
            should_replace_view = current_columns != gold_frames[output_name].columns
        if should_replace_view:
            spark.sql(build_publication_view_sql(
                catalog,
                gold_schema,
                output_name,
                gold_frames[output_name].columns,
            ))

    return active_publication_id(spark, catalog, gold_schema)



# COMMAND ----------

source_tables = {
    "deliveries": "silver_deliveries",
    "payments": "silver_payments",
    "order_status": "silver_order_status",
    "ratings": "silver_ratings",
    "drivers": "drivers",
}

output_counts = {}
publication_committed = False
try:
    silver_frames = {
        key: spark.table(f"{catalog}.{silver_schema}.{table_name}")
        for key, table_name in source_tables.items()
    }
    gold_frames = build_gold_outputs(silver_frames)

    # Quality observations are measured from the same validated Gold frames
    # that are about to be published. Historical anomalies are advisory signals;
    # the hard correctness gates above remain the publication gate.
    quality_metrics_table = f"{catalog}.{gold_schema}.gold_quality_metrics"
    quality_observations = build_gold_quality_observations(
        gold_frames,
        run_id=run_id,
        pipeline_run_id=pipeline_run_id,
        recorded_at_utc=started_at,
    )
    if spark.catalog.tableExists(quality_metrics_table):
        quality_history = spark.table(quality_metrics_table)
    else:
        quality_history = spark.createDataFrame([], quality_metrics_schema())

    quality_results = evaluate_quality_anomalies(
        quality_observations,
        quality_history,
        min_history_observations=quality_min_history_runs,
        z_score_threshold=quality_z_score_threshold,
        relative_change_threshold=quality_relative_change_threshold,
        absolute_floor=quality_absolute_floor,
    ).localCheckpoint(eager=True)
    quality_metric_count = int(quality_results.count())
    quality_anomaly_count = int(
        quality_results.filter("status = 'ANOMALY'").count()
    )

    # Establish stable consumer views over versioned physical tables. Existing
    # legacy Gold tables are retained under __legacy_* names during bootstrap.
    expected_previous_publication_id = _ensure_publication_views(gold_frames)
    _append_publication_event(
        run_id,
        "STAGING_STARTED",
        previous_publication_id=expected_previous_publication_id,
    )

    # Stage and reconcile every output before the single active-pointer commit.
    for output_name in GOLD_OUTPUT_NAMES:
        output_frame = gold_frames[output_name]
        version_table = _raw_table_name(history_table_name(output_name))
        expected_count = int(output_frame.count())
        (
            with_publication_id(output_frame, run_id)
            .write.format("delta")
            .mode("append")
            .saveAsTable(version_table)
        )
        actual_count = (
            spark.table(version_table)
            .filter(F.col("_publication_id") == F.lit(run_id))
            .count()
        )
        if int(actual_count) != expected_count:
            raise RuntimeError(
                f"GOLD_PUBLICATION_STAGE_COUNT_MISMATCH for {output_name}: "
                f"expected {expected_count}, found {actual_count}"
            )
        output_counts[output_name] = expected_count

    _append_publication_event(
        run_id,
        "SNAPSHOTS_STAGED",
        previous_publication_id=expected_previous_publication_id,
        output_counts=output_counts,
    )

    # One Delta MERGE is the publication commit point for all five consumer
    # views. The compare-and-swap condition prevents a stale concurrent run
    # from replacing a publication committed after this run started.
    try:
        spark.sql(build_activation_merge_sql(
            catalog,
            gold_schema,
            publication_id=run_id,
            expected_previous_publication_id=expected_previous_publication_id,
            pipeline_run_id=pipeline_run_id,
        ))
        publication_committed = True
    except Exception:
        # A concurrent MERGE may commit before Spark surfaces an optimistic
        # concurrency error; inspect the pointer before classifying the run.
        if active_publication_id(spark, catalog, gold_schema) == run_id:
            publication_committed = True
        else:
            raise

    active_id_after_merge = active_publication_id(spark, catalog, gold_schema)
    if active_id_after_merge != run_id:
        publication_committed = False
        raise RuntimeError(
            "GOLD_PUBLICATION_CONFLICT: active pointer changed after this run "
            "captured its expected predecessor; staged snapshots remain unpublished."
        )

    # The business output is now committed. Monitoring/audit sidecars are
    # deliberately non-blocking after this point so an audit write failure
    # cannot misreport a successful publication as a failed Gold release.
    output_counts["gold_quality_metrics"] = quality_metric_count
    output_counts["quality_anomalies"] = quality_anomaly_count
    post_commit_warnings = []

    try:
        (
            quality_results.write
            .format("delta")
            .mode("append")
            .saveAsTable(quality_metrics_table)
        )
    except Exception as monitoring_error:
        post_commit_warnings.append(
            f"quality metrics append failed: {type(monitoring_error).__name__}: {str(monitoring_error)[:500]}"
        )

    if post_commit_warnings:
        status = "SUCCEEDED_WITH_WARNINGS"
        error_text = " | ".join(post_commit_warnings)
    else:
        status = "SUCCEEDED"
        error_text = None

    try:
        write_audit(status, output_counts, error_text)
    except Exception as audit_error:
        post_commit_warnings.append(
            f"Gold run audit append failed: {type(audit_error).__name__}: {str(audit_error)[:500]}"
        )

    try:
        _append_publication_event(
            run_id,
            "PUBLISHED_WITH_WARNINGS" if post_commit_warnings else "PUBLISHED",
            previous_publication_id=expected_previous_publication_id,
            output_counts=output_counts,
            error_message=" | ".join(post_commit_warnings) if post_commit_warnings else None,
        )
    except Exception as history_error:
        post_commit_warnings.append(
            f"publication history append failed: {type(history_error).__name__}: {str(history_error)[:500]}"
        )

    final_publish_status = "SUCCEEDED_WITH_WARNINGS" if post_commit_warnings else "SUCCEEDED"
    print(
        f"gold.publish.status={final_publish_status}; "
        f"pipeline_run_id={pipeline_run_id}; run_id={run_id}; "
        f"previous_publication_id={expected_previous_publication_id}; "
        f"quality_metrics={quality_metric_count}; quality_anomalies={quality_anomaly_count}; "
        f"outputs={json.dumps(output_counts, sort_keys=True)}"
    )
    if post_commit_warnings:
        print("gold.publish.warnings=" + json.dumps(post_commit_warnings))
except Exception as exc:
    if publication_committed:
        # The single pointer commit returned successfully but a later read or
        # verification call failed. Preserve the published status; do not
        # report this as a failed release or invite a blind retry.
        warning = (
            f"publication pointer commit succeeded; post-commit verification failed: "
            f"{type(exc).__name__}: {str(exc)[:1000]}"
        )
        try:
            write_audit("SUCCEEDED_WITH_WARNINGS", output_counts, warning)
        except Exception as audit_error:
            print(f"gold.audit_write_error={type(audit_error).__name__}: {str(audit_error)[:500]}")
        try:
            _append_publication_event(
                run_id,
                "PUBLISHED_WITH_WARNINGS",
                previous_publication_id=locals().get("expected_previous_publication_id"),
                output_counts=output_counts,
                error_message=warning,
            )
        except Exception as history_error:
            print(f"gold.publication_history_error={type(history_error).__name__}: {str(history_error)[:500]}")
        print(f"gold.publish.status=SUCCEEDED_WITH_WARNINGS; run_id={run_id}; warning={warning}")
    else:
        # Never roll back or overwrite the active pointer in this failure
        # path. Partial version rows are invisible through stable Gold views.
        try:
            _append_publication_event(
                run_id,
                "FAILED_BEFORE_COMMIT",
                previous_publication_id=locals().get("expected_previous_publication_id"),
                output_counts=output_counts,
                error_message=f"{type(exc).__name__}: {str(exc)}",
            )
        except Exception as history_error:
            print(f"gold.publication_history_error={type(history_error).__name__}: {str(history_error)[:500]}")
        try:
            write_audit("FAILED_BEFORE_COMMIT", output_counts, f"{type(exc).__name__}: {exc}")
        except Exception as audit_error:
            print(f"gold.audit_write_error={type(audit_error).__name__}: {str(audit_error)[:500]}")
        raise
