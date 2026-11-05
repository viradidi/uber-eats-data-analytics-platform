# Databricks notebook source
# Metadata-driven SQL incremental Silver publisher.
# ADF calls this notebook after landing one deterministic SQL extraction file.

dbutils.widgets.text("control_id", "")
dbutils.widgets.text("parent_pipeline_run_id", "")
dbutils.widgets.text("child_pipeline_run_id", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("watermark_column", "")
dbutils.widgets.text("lower_watermark", "")
dbutils.widgets.text("upper_watermark", "")
dbutils.widgets.text("business_key_columns_json", "")
dbutils.widgets.text("required_columns_json", "")
dbutils.widgets.text("column_types_json", "")
dbutils.widgets.text("bronze_root", "abfss://bronze@ubereatsrisstorage.dfs.core.windows.net")
dbutils.widgets.text("batch_file_name", "")
dbutils.widgets.text("project_root", "/Workspace/Shared/UberEats", "Shared project code root")

import json
import re
import sys
import uuid
from datetime import datetime, timezone

PROJECT_ROOT = dbutils.widgets.get("project_root").strip()
if PROJECT_ROOT and PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.common.sql_silver import (
    parse_identifier_list,
    parse_column_types,
    prepare_sql_silver_batch,
    publish_delta_merge,
    validate_target_table,
    write_sql_silver_audit,
)


def required_widget(name):
    value = dbutils.widgets.get(name).strip()
    if not value:
        raise ValueError(f"Required ADF parameter is missing: {name}")
    return value


control_id = int(required_widget("control_id"))
schema_name = required_widget("schema_name")
table_name = required_widget("table_name")
target_table = validate_target_table(required_widget("target_table"))
watermark_column = required_widget("watermark_column")
lower_watermark = required_widget("lower_watermark")
upper_watermark = required_widget("upper_watermark")
business_key_columns_json = required_widget("business_key_columns_json")
required_columns_json = required_widget("required_columns_json")
column_types_json = required_widget("column_types_json")
BRONZE_ROOT = required_widget("bronze_root").rstrip("/")
batch_file_name = required_widget("batch_file_name")

for label, value in (("schema_name", schema_name), ("table_name", table_name)):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{label} contains an unsafe identifier")

if "/" in batch_file_name or "\\" in batch_file_name or not batch_file_name.endswith(".csv"):
    raise ValueError("batch_file_name must be a single .csv filename, not a path or glob")

business_key_columns = parse_identifier_list(
    business_key_columns_json, "business_key_columns_json"
)
required_columns = parse_identifier_list(
    required_columns_json, "required_columns_json", allow_empty=True
)
column_types = parse_column_types(column_types_json)

landing_file_path = f"{BRONZE_ROOT}/{table_name}/{batch_file_name}"
parent_pipeline_run_id = dbutils.widgets.get("parent_pipeline_run_id").strip()
child_pipeline_run_id = dbutils.widgets.get("child_pipeline_run_id").strip()
# Prefer the master ADF run ID so SQL Silver audit rows can be joined to Gold audit.
# Fall back to the SQL child run ID for direct child-pipeline execution, then UUID for manual runs.
run_id = parent_pipeline_run_id or child_pipeline_run_id or str(uuid.uuid4())
notebook_execution_id = str(uuid.uuid4())
audit_base = {
    "run_id": run_id,
    "control_id": control_id,
    "source_schema": schema_name,
    "source_table": table_name,
    "target_table": target_table,
    "batch_file_name": batch_file_name,
    "lower_watermark": lower_watermark,
    "upper_watermark": upper_watermark,
    "source_row_count": None,
    "target_row_count": None,
    "missing_source_keys": None,
    "status": "FAILED",
    "error_message": None,
    "recorded_at_utc": datetime.now(timezone.utc).replace(tzinfo=None),
}

try:
    print(
        f"SQL Silver run_id={run_id}; "
        f"parent_pipeline_run_id={parent_pipeline_run_id or '<not-provided>'}; "
        f"child_pipeline_run_id={child_pipeline_run_id or '<not-provided>'}; "
        f"notebook_execution_id={notebook_execution_id}"
    )
    print(f"SQL Silver landing_file={landing_file_path}")
    print(f"SQL Silver target={target_table}")

    raw_batch = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .option("encoding", "UTF-8")
        .load(f"{BRONZE_ROOT}/{table_name}/{batch_file_name}")
    )

    prepared_batch, validation_metrics = prepare_sql_silver_batch(
        raw_batch,
        business_key_columns=business_key_columns,
        required_columns=required_columns,
        column_types=column_types,
        watermark_column=watermark_column,
        lower_watermark=lower_watermark,
        upper_watermark=upper_watermark,
    )
    audit_base["source_row_count"] = validation_metrics["source_rows"]

    publish_metrics = publish_delta_merge(
        spark,
        prepared_batch,
        target_table=target_table,
        business_key_columns=business_key_columns,
        watermark_column=watermark_column,
    )
    audit_base["target_row_count"] = publish_metrics["target_rows_after"]
    audit_base["missing_source_keys"] = publish_metrics["missing_source_keys"]
    audit_base["status"] = "SUCCEEDED"

    audit_table = write_sql_silver_audit(
        spark, target_table=target_table, record=audit_base
    )
    print(json.dumps({
        "run_id": run_id,
        "status": "SUCCEEDED",
        "source_rows": validation_metrics["source_rows"],
        "target_rows_after": publish_metrics["target_rows_after"],
        "missing_source_keys": publish_metrics["missing_source_keys"],
        "audit_table": audit_table,
    }, sort_keys=True))
except Exception as exc:
    audit_base["error_message"] = f"{type(exc).__name__}: {str(exc)}"[:2000]
    try:
        audit_table = write_sql_silver_audit(
            spark, target_table=target_table, record=audit_base
        )
        print(f"SQL Silver failure outcome appended to {audit_table}")
    except Exception as audit_exc:
        print(f"SQL Silver failure-audit write also failed: {type(audit_exc).__name__}: {audit_exc}")
    raise
