"""Safe versioned publication primitives for the Gold analytics layer.

Gold business outputs are kept in append-only version tables. Stable consumer
views resolve all five outputs through one Delta publication-state row. A
single compare-and-swap MERGE switches that row only after every snapshot is
written and reconciled.
"""
from __future__ import annotations

import re

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PUBLICATION_KEY = "gold_analytics"
GOLD_OUTPUT_NAMES = (
    "gold_order_360",
    "gold_daily_order_performance",
    "gold_payment_method_performance",
    "gold_restaurant_feedback",
    "gold_driver_performance",
)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_identifier(value: str, label: str = "identifier") -> str:
    """Validate a Unity Catalog identifier before embedding it in SQL."""
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe {label}: {value!r}")
    return value


def qualified_name(catalog: str, schema: str, table: str) -> str:
    """Return a fully qualified, quoted Unity Catalog table identifier."""
    return ".".join(
        f"`{validate_identifier(part, 'table identifier')}`"
        for part in (catalog, schema, table)
    )


def history_table_name(output_name: str) -> str:
    """Return the append-only physical table backing a stable Gold view."""
    if output_name not in GOLD_OUTPUT_NAMES:
        raise ValueError(f"Unsupported Gold output: {output_name!r}")
    return f"{output_name}__versions"


def publication_state_schema() -> StructType:
    return StructType([
        StructField("publication_key", StringType(), False),
        StructField("active_publication_id", StringType(), True),
        StructField("updated_at_utc", TimestampType(), True),
        StructField("pipeline_run_id", StringType(), True),
    ])


def publication_history_schema() -> StructType:
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("publication_id", StringType(), False),
        StructField("pipeline_run_id", StringType(), False),
        StructField("event_status", StringType(), False),
        StructField("recorded_at_utc", TimestampType(), False),
        StructField("previous_publication_id", StringType(), True),
        StructField("output_counts_json", StringType(), False),
        StructField("error_message", StringType(), True),
    ])


def with_publication_id(frame: DataFrame, publication_id: str) -> DataFrame:
    """Tag a validated Gold output with the publication version it belongs to."""
    if not publication_id or not isinstance(publication_id, str):
        raise ValueError("publication_id must be a non-empty string.")
    if "_publication_id" in frame.columns:
        raise ValueError("Gold output already contains reserved _publication_id column.")
    return frame.withColumn("_publication_id", F.lit(publication_id).cast("string"))


def build_publication_view_sql(
    catalog: str,
    schema: str,
    output_name: str,
    columns: tuple[str, ...] | list[str],
) -> str:
    """Build a stable consumer view pinned to the single active publication ID."""
    if output_name not in GOLD_OUTPUT_NAMES:
        raise ValueError(f"Unsupported Gold output: {output_name!r}")
    if not columns:
        raise ValueError("Gold view requires at least one output column.")
    for column in columns:
        validate_identifier(column, "Gold output column")

    view_name = qualified_name(catalog, schema, output_name)
    versions_name = qualified_name(catalog, schema, history_table_name(output_name))
    state_name = qualified_name(catalog, schema, "gold_publication_state")
    projection = ",\n    ".join(f"v.`{column}`" for column in columns)
    return (
        f"CREATE OR REPLACE VIEW {view_name} AS\n"
        f"SELECT\n    {projection}\n"
        f"FROM {versions_name} AS v\n"
        f"INNER JOIN {state_name} AS p\n"
        f"    ON p.publication_key = '{PUBLICATION_KEY}'\n"
        "   AND v._publication_id = p.active_publication_id"
    )


def build_activation_merge_sql(
    catalog: str,
    schema: str,
    *,
    publication_id: str,
    expected_previous_publication_id: str | None,
    pipeline_run_id: str,
) -> str:
    """Build a compare-and-swap MERGE for the single active publication pointer.

    A concurrent publisher that started from a stale pointer cannot overwrite
    the newer publication: its matched update predicate will no longer hold.
    """
    if not publication_id or not isinstance(publication_id, str):
        raise ValueError("publication_id must be a non-empty string.")
    if not pipeline_run_id or not isinstance(pipeline_run_id, str):
        raise ValueError("pipeline_run_id must be a non-empty string.")

    table = qualified_name(catalog, schema, "gold_publication_state")
    expected_sql = (
        "CAST(NULL AS STRING)"
        if expected_previous_publication_id is None
        else "'" + expected_previous_publication_id.replace("'", "''") + "'"
    )
    new_id_sql = "'" + publication_id.replace("'", "''") + "'"
    pipeline_sql = "'" + pipeline_run_id.replace("'", "''") + "'"

    return f"""MERGE INTO {table} AS target
USING (
    SELECT
        '{PUBLICATION_KEY}' AS publication_key,
        {new_id_sql} AS new_publication_id,
        {expected_sql} AS expected_previous_publication_id,
        {pipeline_sql} AS pipeline_run_id
) AS source
ON target.publication_key = source.publication_key
WHEN MATCHED
 AND target.active_publication_id <=> source.expected_previous_publication_id
THEN UPDATE SET
    target.active_publication_id = source.new_publication_id,
    target.updated_at_utc = current_timestamp(),
    target.pipeline_run_id = source.pipeline_run_id
WHEN NOT MATCHED
 AND source.expected_previous_publication_id IS NULL
THEN INSERT (
    publication_key,
    active_publication_id,
    updated_at_utc,
    pipeline_run_id
)
VALUES (
    source.publication_key,
    source.new_publication_id,
    current_timestamp(),
    source.pipeline_run_id
)"""


def active_publication_id(spark, catalog: str, schema: str) -> str | None:
    """Read and validate the sole active Gold publication pointer."""
    table = ".".join((
        validate_identifier(catalog, "catalog"),
        validate_identifier(schema, "schema"),
        "gold_publication_state",
    ))
    if not spark.catalog.tableExists(table):
        return None
    rows = (
        spark.table(table)
        .filter(F.col("publication_key") == F.lit(PUBLICATION_KEY))
        .select("active_publication_id")
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise RuntimeError("Gold publication state contains duplicate publication keys.")
    return rows[0]["active_publication_id"] if rows else None
