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

from pyspark.sql.types import StringType, StructField, StructType

from src.gold.transformations import build_gold_outputs
from src.gold.quality import (
    build_gold_quality_observations,
    evaluate_quality_anomalies,
    quality_metrics_schema,
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


# COMMAND ----------

source_tables = {
    "deliveries": "silver_deliveries",
    "payments": "silver_payments",
    "order_status": "silver_order_status",
    "ratings": "silver_ratings",
    "drivers": "drivers",
}

output_counts = {}
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

    for output_name, output_frame in gold_frames.items():
        target_name = f"{catalog}.{gold_schema}.{output_name}"
        (
            output_frame.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_name)
        )
        output_counts[output_name] = int(spark.table(target_name).count())

    (
        quality_results.write
        .format("delta")
        .mode("append")
        .saveAsTable(quality_metrics_table)
    )
    output_counts["gold_quality_metrics"] = quality_metric_count
    output_counts["quality_anomalies"] = quality_anomaly_count

    write_audit("SUCCEEDED", output_counts)
    print(
        "gold.publish.status=SUCCEEDED; "
        f"pipeline_run_id={pipeline_run_id}; run_id={run_id}; "
        f"quality_metrics={quality_metric_count}; quality_anomalies={quality_anomaly_count}; "
        f"outputs={json.dumps(output_counts, sort_keys=True)}"
    )
except Exception as exc:
    try:
        write_audit("FAILED", output_counts, f"{type(exc).__name__}: {exc}")
    except Exception as audit_error:
        print(f"gold.audit_write_error={type(audit_error).__name__}: {str(audit_error)[:500]}")
    raise
