"""Replay-safe preparation for cumulative API snapshot folders.

ADF writes API extracts as timestamped landing files. Silver notebooks read the
whole endpoint folder, so the same source record can appear in multiple files.
This helper collapses exact replays and resolves changed entities by a reliable
source update timestamp before a full-snapshot Silver overwrite.
"""
from __future__ import annotations

from functools import reduce
from operator import or_

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql.window import Window


def deduplicate_replayed_snapshot_rows(
    frame: DataFrame,
    key_columns: list[str],
    version_column: str | None = None,
    dataset_name: str = "source",
) -> DataFrame:
    """Return one deterministic row per business key from cumulative snapshots.

    When version_column is supplied, the row with the greatest parseable source
    update timestamp wins. Two different payloads for the same key and version
    are rejected because no deterministic winner can be justified.

    Without a version column, only exact replays are collapsed. Conflicting
    payloads for the same key are rejected rather than resolved arbitrarily.
    """
    if not key_columns:
        raise ValueError("key_columns must contain at least one business key")
    if len(key_columns) != len(set(key_columns)):
        raise ValueError("key_columns must not contain duplicates")

    required = set(key_columns)
    if version_column:
        required.add(version_column)
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{dataset_name} replay input is missing required columns: {missing}")

    null_key_condition = reduce(or_, (F.col(name).isNull() for name in key_columns))
    if frame.filter(null_key_condition).limit(1).count():
        raise ValueError(f"{dataset_name} replay input contains null business keys: {key_columns}")

    payload_json = F.to_json(F.struct(*[F.col(name) for name in frame.columns]))
    if version_column:
        version_field = frame.schema[version_column].dataType
        if isinstance(version_field, TimestampType):
            version_expr = F.col(version_column)
        elif isinstance(version_field, StringType):
            version_expr = F.to_timestamp(F.col(version_column))
        else:
            version_expr = F.col(version_column).cast("timestamp")

        prepared = frame.withColumn("_replay_version_ts", version_expr)
        if prepared.filter(F.col("_replay_version_ts").isNull()).limit(1).count():
            raise ValueError(
                f"{dataset_name} replay input contains null or unparseable {version_column} values"
            )

        conflict = (
            prepared
            .groupBy(*(key_columns + ["_replay_version_ts"]))
            .agg(F.countDistinct(payload_json).alias("_payload_versions"))
            .filter(F.col("_payload_versions") > 1)
            .limit(1)
            .count()
        )
        if conflict:
            raise ValueError(
                f"{dataset_name} replay input contains conflicting payloads for the same "
                f"business key and {version_column}"
            )

        before = prepared.count()
        exact_distinct = prepared.dropDuplicates(frame.columns)
        winner_window = Window.partitionBy(*key_columns).orderBy(
            F.col("_replay_version_ts").desc()
        )
        result = (
            exact_distinct
            .withColumn("_replay_row_number", F.row_number().over(winner_window))
            .filter(F.col("_replay_row_number") == 1)
            .drop("_replay_row_number", "_replay_version_ts")
        )
    else:
        conflict = (
            frame
            .groupBy(*key_columns)
            .agg(F.countDistinct(payload_json).alias("_payload_versions"))
            .filter(F.col("_payload_versions") > 1)
            .limit(1)
            .count()
        )
        if conflict:
            raise ValueError(
                f"{dataset_name} replay input contains conflicting payloads for the same "
                f"business key; no source version column was supplied"
            )

        before = frame.count()
        result = frame.dropDuplicates(key_columns)

    after = result.count()
    print(
        f"{dataset_name}.replay_deduplication."
        f"input_rows={before}; output_rows={after}; removed_replays={before - after}"
    )
    return result
