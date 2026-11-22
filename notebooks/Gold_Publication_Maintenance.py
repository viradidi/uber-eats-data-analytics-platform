# Databricks notebook source
"""Dry-run-first retention and maintenance for versioned Gold publications."""

import json
import re
import sys
from datetime import datetime, timezone
from uuid import uuid4

dbutils.widgets.text("project_root", "/Workspace/Shared/UberEats", "Workspace package root")
PROJECT_ROOT = dbutils.widgets.get("project_root").strip().rstrip("/")
if PROJECT_ROOT and PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.gold.publication import (
    GOLD_OUTPUT_NAMES,
    active_publication_id,
    history_table_name,
    qualified_name,
)
from src.gold.retention import (
    MIN_RETENTION_DAYS,
    build_guarded_delete_sql,
    build_retention_plan,
)


# COMMAND ----------

dbutils.widgets.text("catalog", "ubereats_databricks1", "Unity Catalog catalog")
dbutils.widgets.text("gold_schema", "gold", "Gold schema")
dbutils.widgets.text("retention_days", "30", "Minimum logical retention window")
dbutils.widgets.text("keep_successful_publications", "10", "Successful releases to retain")
dbutils.widgets.text("dry_run", "true", "Plan only; set false to delete")
dbutils.widgets.text("confirmation", "", "Type DELETE_OLD_GOLD_PUBLICATIONS to execute")
dbutils.widgets.text("optimize_after_cleanup", "false", "Run OPTIMIZE after deletes")
dbutils.widgets.text("vacuum_after_cleanup", "false", "Run VACUUM after deletes")
dbutils.widgets.text("vacuum_retention_hours", "168", "VACUUM data-file retention hours (minimum 168)")

catalog = dbutils.widgets.get("catalog").strip()
gold_schema = dbutils.widgets.get("gold_schema").strip()
retention_days = int(dbutils.widgets.get("retention_days"))
keep_successful_publications = int(dbutils.widgets.get("keep_successful_publications"))
dry_run_text = dbutils.widgets.get("dry_run").strip().lower()
confirmation = dbutils.widgets.get("confirmation").strip()
optimize_after_cleanup = dbutils.widgets.get("optimize_after_cleanup").strip().lower()
vacuum_after_cleanup = dbutils.widgets.get("vacuum_after_cleanup").strip().lower()
vacuum_retention_hours = int(dbutils.widgets.get("vacuum_retention_hours"))

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
for label, value in (("catalog", catalog), ("gold_schema", gold_schema)):
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe {label} identifier: {value!r}")

if retention_days < MIN_RETENTION_DAYS:
    raise ValueError(f"retention_days must be at least {MIN_RETENTION_DAYS}.")
if keep_successful_publications < 1:
    raise ValueError("keep_successful_publications must be at least 1.")
if dry_run_text not in {"true", "false"}:
    raise ValueError("dry_run must be either true or false.")
if optimize_after_cleanup not in {"true", "false"}:
    raise ValueError("optimize_after_cleanup must be either true or false.")
if vacuum_after_cleanup not in {"true", "false"}:
    raise ValueError("vacuum_after_cleanup must be either true or false.")
if vacuum_retention_hours < 168:
    raise ValueError("vacuum_retention_hours must be at least 168 (7 days).")

dry_run = dry_run_text == "true"
optimize_after_cleanup = optimize_after_cleanup == "true"
vacuum_after_cleanup = vacuum_after_cleanup == "true"
if not dry_run and confirmation != "DELETE_OLD_GOLD_PUBLICATIONS":
    raise ValueError(
        "Destructive cleanup requires dry_run=false and confirmation="
        "DELETE_OLD_GOLD_PUBLICATIONS."
    )

state_table = f"{catalog}.{gold_schema}.gold_publication_state"
history_table = f"{catalog}.{gold_schema}.gold_publication_history"
retention_audit_table = f"{catalog}.{gold_schema}.gold_publication_retention_runs"
version_tables = {
    output_name: f"{catalog}.{gold_schema}.{history_table_name(output_name)}"
    for output_name in GOLD_OUTPUT_NAMES
}

required_tables = [state_table, history_table, *version_tables.values()]
missing_tables = [name for name in required_tables if not spark.catalog.tableExists(name)]
if missing_tables:
    raise RuntimeError(
        "GOLD_RETENTION_PREREQUISITE_MISSING: publication state/history/version "
        f"tables must all exist before cleanup: {missing_tables}"
    )

if active_publication_id(spark, catalog, gold_schema) is None:
    raise RuntimeError(
        "GOLD_RETENTION_NO_ACTIVE_PUBLICATION: refusing cleanup without an active Gold pointer."
    )

audit_schema = StructType([
    StructField("retention_run_id", StringType(), False),
    StructField("recorded_at_utc", TimestampType(), False),
    StructField("catalog_name", StringType(), False),
    StructField("gold_schema", StringType(), False),
    StructField("dry_run", BooleanType(), False),
    StructField("retention_days", IntegerType(), False),
    StructField("keep_successful_publications", IntegerType(), False),
    StructField("active_publication_id", StringType(), True),
    StructField("cutoff_utc", StringType(), True),
    StructField("candidate_publication_ids_json", StringType(), False),
    StructField("inventory_publication_count", IntegerType(), False),
    StructField("row_counts_json", StringType(), False),
    StructField("optimize_requested", BooleanType(), False),
    StructField("vacuum_requested", BooleanType(), False),
    StructField("vacuum_retention_hours", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True),
])

if not spark.catalog.tableExists(retention_audit_table):
    (
        spark.createDataFrame([], schema=audit_schema)
        .write.format("delta")
        .mode("overwrite")
        .saveAsTable(retention_audit_table)
    )


def write_retention_audit(
    status,
    active_id,
    cutoff_utc,
    candidates,
    inventory_count,
    row_counts,
    error_message=None,
):
    record = [(
        retention_run_id,
        datetime.now(timezone.utc),
        catalog,
        gold_schema,
        dry_run,
        retention_days,
        keep_successful_publications,
        active_id,
        cutoff_utc,
        json.dumps(candidates, sort_keys=True),
        int(inventory_count),
        json.dumps(row_counts, sort_keys=True),
        optimize_after_cleanup,
        vacuum_after_cleanup,
        vacuum_retention_hours,
        status,
        error_message[:2000] if error_message else None,
    )]
    (
        spark.createDataFrame(record, schema=audit_schema)
        .write.format("delta")
        .mode("append")
        .saveAsTable(retention_audit_table)
    )


# COMMAND ----------

retention_run_id = str(uuid4())
recorded_now = datetime.now(timezone.utc)


def read_version_inventory():
    publication_ids = set()
    for table_name in version_tables.values():
        ids = (
            spark.table(table_name)
            .select("_publication_id")
            .where(F.col("_publication_id").isNotNull())
            .distinct()
            .collect()
        )
        publication_ids.update(row["_publication_id"] for row in ids)
    return sorted(publication_ids)


def calculate_plan():
    active_id = active_publication_id(spark, catalog, gold_schema)
    if active_id is None:
        raise RuntimeError("Gold active publication pointer disappeared during retention planning.")
    history_rows = (
        spark.table(history_table)
        .select("publication_id", "event_status", "recorded_at_utc")
        .collect()
    )
    inventory = read_version_inventory()
    plan = build_retention_plan(
        history_rows,
        inventory,
        active_publication_id=active_id,
        now_utc=recorded_now,
        retention_days=retention_days,
        keep_successful_publications=keep_successful_publications,
    )
    return active_id, inventory, plan


active_id, inventory, plan = calculate_plan()
candidate_ids = plan["candidate_publication_ids"]
planned_counts = {
    output_name: int(
        spark.table(table_name)
        .filter(F.col("_publication_id").isin(candidate_ids))
        .count()
    ) if candidate_ids else 0
    for output_name, table_name in version_tables.items()
}

print(json.dumps({
    "retention_run_id": retention_run_id,
    "dry_run": dry_run,
    "active_publication_id": active_id,
    "retention_days": retention_days,
    "keep_successful_publications": keep_successful_publications,
    "cutoff_utc": plan["cutoff_utc"],
    "inventory_publication_count": plan["inventory_publication_count"],
    "candidate_publication_count": len(candidate_ids),
    "candidate_publication_ids": candidate_ids,
    "preserved_publications": plan["preserved"],
    "planned_rows_by_output": planned_counts,
}, sort_keys=True, default=str))

try:
    final_counts = {}
    final_status = "DRY_RUN"
    final_active_id = active_id
    final_cutoff = plan["cutoff_utc"]
    final_candidates = candidate_ids
    final_inventory_count = plan["inventory_publication_count"]

    if not dry_run:
        # Recompute the inventory and active pointer immediately before
        # deletion. DELETE statements independently exclude the current active
        # pointer, so the active snapshot cannot be removed by this job.
        final_active_id, final_inventory, final_plan = calculate_plan()
        final_candidates = final_plan["candidate_publication_ids"]
        final_cutoff = final_plan["cutoff_utc"]
        final_inventory_count = final_plan["inventory_publication_count"]

        for output_name, table_name in version_tables.items():
            table_counts = {
                "rows_before": int(
                    spark.table(table_name)
                    .filter(F.col("_publication_id").isin(final_candidates))
                    .count()
                ) if final_candidates else 0,
                "rows_deleted": 0,
                "rows_remaining": 0,
            }
            for start in range(0, len(final_candidates), 500):
                batch = final_candidates[start : start + 500]
                delete_sql = build_guarded_delete_sql(
                    catalog,
                    gold_schema,
                    output_name,
                    batch,
                )
                if delete_sql:
                    spark.sql(delete_sql)

            remaining = (
                spark.table(table_name)
                .filter(F.col("_publication_id").isin(final_candidates))
                .groupBy("_publication_id")
                .count()
                .collect()
            ) if final_candidates else []
            table_counts["rows_remaining"] = sum(int(row["count"]) for row in remaining)
            table_counts["rows_deleted"] = (
                table_counts["rows_before"] - table_counts["rows_remaining"]
            )
            final_counts[output_name] = table_counts

        if optimize_after_cleanup:
            for table_name in version_tables.values():
                spark.sql(f"OPTIMIZE {qualified_name(catalog, gold_schema, table_name.split('.')[-1])}")

        if vacuum_after_cleanup:
            for table_name in version_tables.values():
                spark.sql(
                    f"VACUUM {qualified_name(catalog, gold_schema, table_name.split('.')[-1])} "
                    f"RETAIN {vacuum_retention_hours} HOURS"
                )

        remaining_total = sum(
            counts["rows_remaining"] for counts in final_counts.values()
        )
        final_status = "COMPLETED_WITH_REMAINDERS" if remaining_total else "COMPLETED"

    write_retention_audit(
        final_status,
        final_active_id,
        final_cutoff,
        final_candidates,
        final_inventory_count,
        planned_counts if dry_run else final_counts,
    )
    print(
        "gold.retention.status="
        f"{final_status}; retention_run_id={retention_run_id}; "
        f"candidate_publications={len(final_candidates)}; "
        f"active_publication_id={final_active_id}"
    )
except Exception as exc:
    try:
        write_retention_audit(
            "FAILED_OR_PARTIAL",
            active_id,
            plan["cutoff_utc"],
            candidate_ids,
            plan["inventory_publication_count"],
            planned_counts,
            f"{type(exc).__name__}: {exc}",
        )
    except Exception as audit_error:
        print(
            "gold.retention.audit_error="
            f"{type(audit_error).__name__}: {str(audit_error)[:500]}"
        )
    raise
