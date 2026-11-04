# Databricks notebook source
import sys

dbutils.widgets.text("project_root", "/Workspace/Shared/UberEats", "Workspace package root")
PROJECT_ROOT = dbutils.widgets.get("project_root").strip().rstrip("/")
if PROJECT_ROOT and PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

"""Publish the Uber Eats Gold analytics layer from validated Silver tables."""

from datetime import datetime, timezone
import json
import re
from uuid import uuid4

from pyspark.sql.types import StringType, StructField, StructType

from src.gold.transformations import build_gold_outputs


# COMMAND ----------

dbutils.widgets.text("catalog", "ubereats_databricks1", "Unity Catalog catalog")
dbutils.widgets.text("silver_schema", "silver", "Silver schema")
dbutils.widgets.text("gold_schema", "gold", "Gold schema")
dbutils.widgets.text("pipeline_run_id", "manual", "Parent ADF pipeline run ID")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id") or "manual"

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

    write_audit("SUCCEEDED", output_counts)
    print(
        "gold.publish.status=SUCCEEDED; "
        f"pipeline_run_id={pipeline_run_id}; run_id={run_id}; "
        f"outputs={json.dumps(output_counts, sort_keys=True)}"
    )
except Exception as exc:
    try:
        write_audit("FAILED", output_counts, f"{type(exc).__name__}: {exc}")
    except Exception as audit_error:
        print(f"gold.audit_write_error={type(audit_error).__name__}: {str(audit_error)[:500]}")
    raise
