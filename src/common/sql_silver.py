"""Metadata-driven, replay-safe SQL batch publication helpers for Databricks.

The SQL Silver contract is deliberately fail-closed: each source must declare
its business key, required fields, and complete Spark type map before a batch
can be published.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TARGET_TABLE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$"
)
_DECIMAL = re.compile(r"^decimal\(([1-9][0-9]?),([0-9]{1,2})\)$")
_ALLOWED_TYPES = {
    "string", "boolean", "byte", "short", "integer", "int", "long", "bigint",
    "float", "double", "date", "timestamp", "binary",
}


def _json_value(value: str | Mapping[str, Any] | Sequence[Any], name: str) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{name} must contain valid JSON: {exc.msg}") from exc
    return value


def parse_identifier_list(value: str | Sequence[str], name: str, *, allow_empty: bool = False) -> list[str]:
    parsed = _json_value(value, name)
    if not isinstance(parsed, list) or any(not isinstance(item, str) for item in parsed):
        raise ValueError(f"{name} must be a JSON array of column-name strings")
    items = [item.strip() for item in parsed]
    if not allow_empty and not items:
        raise ValueError(f"{name} must contain at least one column")
    if len(items) != len(set(items)):
        raise ValueError(f"{name} contains duplicate column names")
    invalid = [item for item in items if not _IDENTIFIER.fullmatch(item)]
    if invalid:
        raise ValueError(f"{name} contains unsafe column identifiers: {invalid[:5]}")
    return items


def parse_column_types(value: str | Mapping[str, str]) -> dict[str, str]:
    parsed = _json_value(value, "column_types_json")
    if not isinstance(parsed, dict) or not parsed:
        raise ValueError("column_types_json must be a non-empty JSON object")
    normalized: dict[str, str] = {}
    for column, raw_type in parsed.items():
        if not isinstance(column, str) or not _IDENTIFIER.fullmatch(column):
            raise ValueError(f"Unsafe column identifier in column_types_json: {column!r}")
        if not isinstance(raw_type, str):
            raise ValueError(f"Spark type for column {column!r} must be a string")
        spark_type = raw_type.strip().lower()
        decimal_match = _DECIMAL.fullmatch(spark_type)
        if spark_type not in _ALLOWED_TYPES and not decimal_match:
            raise ValueError(
                f"Unsupported Spark type {raw_type!r} for column {column!r}"
            )
        if decimal_match:
            precision, scale = map(int, decimal_match.groups())
            if precision > 38 or scale > precision:
                raise ValueError(f"Invalid decimal precision/scale for {column!r}")
        normalized[column] = spark_type
    return normalized


def validate_target_table(target_table: str) -> str:
    value = str(target_table or "").strip()
    if not _TARGET_TABLE.fullmatch(value):
        raise ValueError(
            "target_table must be a fully qualified catalog.schema.table using "
            "letters, digits, and underscores"
        )
    return value


def parse_watermark_bound(value: str | datetime, name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if not text:
            raise ValueError(f"{name} is required")
        fractional = re.search(r"\\.(\\d+)", text)
        if fractional and len(fractional.group(1)) > 6 and any(
            digit != "0" for digit in fractional.group(1)[6:]
        ):
            raise ValueError(
                f"{name} has sub-microsecond precision that Spark TimestampType cannot preserve"
            )
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{name} is not a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def prepare_sql_silver_batch(
    raw_df: DataFrame,
    *,
    business_key_columns: str | Sequence[str],
    required_columns: str | Sequence[str],
    column_types: str | Mapping[str, str],
    watermark_column: str,
    lower_watermark: str | datetime,
    upper_watermark: str | datetime,
) -> tuple[DataFrame, dict[str, int]]:
    """Cast and validate one exact SQL landing file; never silently drop rows."""
    keys = parse_identifier_list(business_key_columns, "business_key_columns_json")
    required = parse_identifier_list(required_columns, "required_columns_json", allow_empty=True)
    types = parse_column_types(column_types)
    watermark = str(watermark_column or "").strip()
    if not _IDENTIFIER.fullmatch(watermark):
        raise ValueError("watermark_column is missing or unsafe")
    if watermark not in types or types[watermark] != "timestamp":
        raise ValueError("The configured watermark column must have Spark type 'timestamp'")
    if not set(keys).issubset(types):
        raise ValueError("Every business key must appear in column_types_json")
    required_set = set(required) | set(keys) | {watermark}
    if not required_set.issubset(types):
        raise ValueError("Required, key, and watermark columns must all be typed")

    source_columns = list(raw_df.columns)
    if len(source_columns) != len(set(source_columns)):
        raise ValueError("The SQL landing CSV contains duplicate header names")
    if set(source_columns) != set(types):
        missing = sorted(set(types) - set(source_columns))
        unexpected = sorted(set(source_columns) - set(types))
        raise ValueError(
            f"SQL landing header does not match its approved contract; "
            f"missing={missing[:10]}, unexpected={unexpected[:10]}"
        )

    lower_bound = parse_watermark_bound(lower_watermark, "lower_watermark")
    upper_bound = parse_watermark_bound(upper_watermark, "upper_watermark")
    if lower_bound >= upper_bound:
        raise ValueError("lower_watermark must be earlier than upper_watermark")

    # Spark TimestampType stores microseconds. Refuse any non-zero seventh
    # fractional digit rather than silently rounding a SQL datetime2(7) cursor.
    for column, spark_type in types.items():
        if spark_type != "timestamp":
            continue
        fractional = F.regexp_extract(F.col(f"`{column}`"), r"\\.(\\d+)", 1)
        has_lost_precision = (
            (F.length(fractional) > 6)
            & F.substring(fractional, 7, 100).rlike("[1-9]")
        )
        if raw_df.filter(has_lost_precision).limit(1).count():
            raise ValueError(
                f"Column {column!r} contains non-zero sub-microsecond digits; "
                "Spark TimestampType cannot preserve that source precision"
            )

    casted = raw_df.select(
        *[
            F.expr(f"try_cast(`{column}` AS {types[column]})").alias(column)
            for column in types
        ]
    )

    invalid_cast_conditions = []
    for column, spark_type in types.items():
        if spark_type == "string":
            continue
        raw_value = F.col(f"`{column}`")
        invalid_cast_conditions.append(
            raw_value.isNotNull()
            & (F.trim(raw_value) != "")
            & F.expr(f"try_cast(`{column}` AS {spark_type})").isNull()
        )
    if invalid_cast_conditions:
        invalid_cast = invalid_cast_conditions[0]
        for condition in invalid_cast_conditions[1:]:
            invalid_cast = invalid_cast | condition
        invalid_cast_count = raw_df.filter(invalid_cast).count()
        if invalid_cast_count:
            raise ValueError(
                f"SQL batch has {invalid_cast_count} non-empty values that fail "
                "the approved column type contract"
            )

    missing_required_conditions = []
    for column in sorted(required_set):
        missing = F.col(f"`{column}`").isNull()
        if types[column] == "string":
            missing = missing | (F.trim(F.col(f"`{column}`")) == "")
        missing_required_conditions.append(missing)
    missing_required = missing_required_conditions[0]
    for condition in missing_required_conditions[1:]:
        missing_required = missing_required | condition
    missing_required_count = casted.filter(missing_required).count()
    if missing_required_count:
        raise ValueError(
            f"SQL batch has {missing_required_count} rows with null/blank required fields"
        )

    row_count = casted.count()
    if row_count == 0:
        raise ValueError("SQL landing batch is empty; refusing to publish")

    out_of_range = casted.filter(
        (F.col(f"`{watermark}`") <= F.lit(lower_bound))
        | (F.col(f"`{watermark}`") > F.lit(upper_bound))
    ).count()
    if out_of_range:
        raise ValueError(
            f"SQL landing file contains {out_of_range} rows outside its declared "
            "exclusive/inclusive watermark interval"
        )

    duplicate_key_groups = (
        casted.groupBy(*[F.col(f"`{key}`") for key in keys])
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if duplicate_key_groups:
        raise ValueError(
            f"SQL batch contains {duplicate_key_groups} duplicate business-key groups; "
            "no arbitrary winner will be selected"
        )

    return casted, {
        "source_rows": int(row_count),
        "duplicate_key_groups": int(duplicate_key_groups),
        "out_of_range_rows": int(out_of_range),
    }


def publish_delta_merge(
    spark: Any,
    batch: DataFrame,
    *,
    target_table: str,
    business_key_columns: Sequence[str],
    watermark_column: str,
) -> dict[str, int]:
    """Publish an idempotent version-aware Delta MERGE and reconcile source keys."""
    from delta.tables import DeltaTable

    target = validate_target_table(target_table)
    keys = list(business_key_columns)
    columns = list(batch.columns)
    if not keys or not set(keys).issubset(columns):
        raise ValueError("A non-empty business key contained in the batch is required")
    if watermark_column not in columns:
        raise ValueError("The watermark column is not present in the batch")

    if not spark.catalog.tableExists(target):
        batch.write.format("delta").mode("errorifexists").saveAsTable(target)
    else:
        existing = spark.table(target)
        target_types = {field.name: field.dataType.simpleString() for field in existing.schema.fields}
        source_types = {field.name: field.dataType.simpleString() for field in batch.schema.fields}
        if target_types != source_types:
            raise ValueError(
                f"Target schema for {target} does not exactly match the approved SQL Silver contract"
            )

        target_rows = existing.count()
        target_duplicate_groups = (
            existing.groupBy(*[F.col(f"`{key}`") for key in keys])
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        if target_duplicate_groups:
            raise ValueError(
                f"Target {target} already contains {target_duplicate_groups} duplicate business-key groups"
            )

        if existing.filter(F.col(f"`{watermark_column}`").isNull()).limit(1).count():
            raise ValueError(
                f"Target {target} contains null values in the version/watermark column"
            )

        match_condition = " AND ".join(
            f"source.`{key}` = target.`{key}`" for key in keys
        )
        joined = batch.alias("source").join(
            existing.alias("target"),
            on=F.expr(match_condition),
            how="inner",
        )
        non_version_columns = [
            column for column in columns
            if column not in set(keys) | {watermark_column}
        ]
        same_version_conflict = None
        if non_version_columns:
            for column in non_version_columns:
                differs = ~F.col(f"source.`{column}`").eqNullSafe(
                    F.col(f"target.`{column}`")
                )
                same_version = (
                    F.col(f"source.`{watermark_column}`")
                    == F.col(f"target.`{watermark_column}`")
                )
                condition = same_version & differs
                same_version_conflict = (
                    condition if same_version_conflict is None
                    else same_version_conflict | condition
                )
        if same_version_conflict is not None:
            conflict_count = joined.filter(same_version_conflict).count()
            if conflict_count:
                raise ValueError(
                    f"SQL batch contains {conflict_count} target keys with conflicting "
                    "payloads at the same watermark version"
                )

        assignments = {column: f"source.`{column}`" for column in columns}
        (
            DeltaTable.forName(spark, target)
            .alias("target")
            .merge(batch.alias("source"), match_condition)
            .whenMatchedUpdate(
                condition=(
                    f"source.`{watermark_column}` > target.`{watermark_column}`"
                ),
                set=assignments,
            )
            .whenNotMatchedInsert(values=assignments)
            .execute()
        )

    published = spark.table(target)
    missing_source_keys = (
        batch.select(*keys).dropDuplicates()
        .join(published.select(*keys).dropDuplicates(), on=keys, how="left_anti")
        .count()
    )
    if missing_source_keys:
        raise RuntimeError(
            f"Post-merge reconciliation found {missing_source_keys} source keys missing from {target}"
        )

    return {
        "target_rows_after": int(published.count()),
        "missing_source_keys": int(missing_source_keys),
    }


def write_sql_silver_audit(
    spark: Any,
    *,
    target_table: str,
    record: Mapping[str, Any],
) -> str:
    """Append an auditable outcome beside the configured target table."""
    from pyspark.sql.types import (
        IntegerType, LongType, StringType, StructField, StructType, TimestampType,
    )

    target = validate_target_table(target_table)
    catalog, schema, _ = target.split(".")
    audit_table = f"{catalog}.{schema}.sql_incremental_run_audit"
    audit_schema = StructType([
        StructField("run_id", StringType(), False),
        StructField("control_id", IntegerType(), False),
        StructField("source_schema", StringType(), False),
        StructField("source_table", StringType(), False),
        StructField("target_table", StringType(), False),
        StructField("batch_file_name", StringType(), False),
        StructField("lower_watermark", StringType(), False),
        StructField("upper_watermark", StringType(), False),
        StructField("source_row_count", LongType(), True),
        StructField("target_row_count", LongType(), True),
        StructField("missing_source_keys", LongType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
        StructField("recorded_at_utc", TimestampType(), False),
    ])
    payload = dict(record)
    payload.setdefault("recorded_at_utc", datetime.now(timezone.utc).replace(tzinfo=None))
    audit_df = spark.createDataFrame([payload], schema=audit_schema)
    audit_df.write.format("delta").mode("append").saveAsTable(audit_table)
    return audit_table
