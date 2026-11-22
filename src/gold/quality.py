"""Run-level Gold quality metrics and conservative historical anomaly detection.

The existing transformation module remains responsible for hard correctness
gates (keys, domains, ranges and reconciliations). This module adds trend
observability: one metric observation per Gold business dimension and run.
Anomaly statuses are advisory and never mutate or reject Gold records.
"""
from __future__ import annotations

from datetime import datetime
from typing import Iterable

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.gold.transformations import FAILED_PAYMENT_STATUSES


OBSERVATION_COLUMNS = (
    "run_id",
    "pipeline_run_id",
    "recorded_at_utc",
    "metric_name",
    "dimension_key",
    "metric_unit",
    "metric_value",
)


def quality_metrics_schema() -> StructType:
    """Stable Delta schema for append-only Gold quality observations."""
    return StructType([
        StructField("run_id", StringType(), False),
        StructField("pipeline_run_id", StringType(), False),
        StructField("recorded_at_utc", TimestampType(), False),
        StructField("metric_name", StringType(), False),
        StructField("dimension_key", StringType(), False),
        StructField("metric_unit", StringType(), False),
        StructField("metric_value", DoubleType(), False),
        StructField("baseline_mean", DoubleType(), True),
        StructField("baseline_stddev", DoubleType(), True),
        StructField("baseline_observation_count", LongType(), False),
        StructField("deviation_ratio", DoubleType(), True),
        StructField("deviation_z_score", DoubleType(), True),
        StructField("status", StringType(), False),
    ])


def _require_columns(frame: DataFrame, columns: Iterable[str], label: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing required columns for quality metrics: {missing}")


def _metric_observations(
    frame: DataFrame,
    *,
    run_id: str,
    pipeline_run_id: str,
    recorded_at_utc: str,
    metric_name: str,
    dimension_expression,
    metric_expression,
    metric_unit: str,
) -> DataFrame:
    return (
        frame.select(
            F.lit(run_id).cast("string").alias("run_id"),
            F.lit(pipeline_run_id).cast("string").alias("pipeline_run_id"),
            F.to_timestamp(F.lit(recorded_at_utc)).alias("recorded_at_utc"),
            F.lit(metric_name).alias("metric_name"),
            dimension_expression.cast("string").alias("dimension_key"),
            F.lit(metric_unit).alias("metric_unit"),
            metric_expression.cast("double").alias("metric_value"),
        )
        .filter(
            F.col("dimension_key").isNotNull()
            & F.col("metric_value").isNotNull()
            & ~F.isnan("metric_value")
        )
    )


def build_gold_quality_observations(
    gold_outputs: dict[str, DataFrame],
    *,
    run_id: str,
    pipeline_run_id: str,
    recorded_at_utc: str,
) -> DataFrame:
    """Create per-day, per-driver, per-restaurant and payment-method metrics."""
    required_outputs = {
        "gold_daily_order_performance",
        "gold_payment_method_performance",
        "gold_driver_performance",
        "gold_restaurant_feedback",
    }
    missing = sorted(required_outputs - set(gold_outputs))
    if missing:
        raise ValueError(f"Gold outputs are missing quality metric inputs: {missing}")

    daily = gold_outputs["gold_daily_order_performance"]
    _require_columns(
        daily,
        (
            "order_date",
            "order_count",
            "delivered_orders",
            "cancelled_orders",
            "paid_orders",
            "successful_payment_amount",
            "average_delivery_minutes",
            "average_customer_rating",
        ),
        "gold_daily_order_performance",
    )
    payments = gold_outputs["gold_payment_method_performance"]
    _require_columns(
        payments,
        ("payment_date", "payment_method", "payment_status", "transaction_count"),
        "gold_payment_method_performance",
    )
    drivers = gold_outputs["gold_driver_performance"]
    _require_columns(
        drivers,
        ("driver_id", "delivery_record_count", "delivery_completion_rate", "average_delivery_minutes"),
        "gold_driver_performance",
    )
    restaurants = gold_outputs["gold_restaurant_feedback"]
    _require_columns(restaurants, ("restaurant_id", "average_rating"), "gold_restaurant_feedback")

    specs = [
        (daily, "daily_order_count", F.col("order_date"), F.col("order_count"), "orders"),
        (
            daily,
            "daily_successful_payment_amount",
            F.col("order_date"),
            F.col("successful_payment_amount"),
            "amount",
        ),
        (
            daily,
            "daily_average_delivery_minutes",
            F.col("order_date"),
            F.col("average_delivery_minutes"),
            "minutes",
        ),
        (
            daily,
            "daily_average_customer_rating",
            F.col("order_date"),
            F.col("average_customer_rating"),
            "rating",
        ),
        (
            daily,
            "daily_delivery_completion_rate",
            F.col("order_date"),
            F.when(
                F.col("order_count") > 0,
                F.col("delivered_orders") / F.col("order_count"),
            ),
            "ratio",
        ),
        (
            daily,
            "daily_cancellation_rate",
            F.col("order_date"),
            F.when(
                F.col("order_count") > 0,
                F.col("cancelled_orders") / F.col("order_count"),
            ),
            "ratio",
        ),
        (
            daily,
            "daily_paid_order_rate",
            F.col("order_date"),
            F.when(
                F.col("order_count") > 0,
                F.col("paid_orders") / F.col("order_count"),
            ),
            "ratio",
        ),
        (
            drivers,
            "driver_delivery_volume",
            F.col("driver_id"),
            F.col("delivery_record_count"),
            "deliveries",
        ),
        (
            drivers,
            "driver_delivery_completion_rate",
            F.col("driver_id"),
            F.col("delivery_completion_rate"),
            "ratio",
        ),
        (
            drivers,
            "driver_average_delivery_minutes",
            F.col("driver_id"),
            F.col("average_delivery_minutes"),
            "minutes",
        ),
        (
            restaurants,
            "restaurant_average_rating",
            F.col("restaurant_id"),
            F.col("average_rating"),
            "rating",
        ),
    ]

    payment_by_method = (
        payments
        .groupBy("payment_date", "payment_method")
        .agg(
            F.sum("transaction_count").alias("transaction_count"),
            F.sum(
                F.when(
                    F.col("payment_status").isin(*FAILED_PAYMENT_STATUSES),
                    F.col("transaction_count"),
                ).otherwise(F.lit(0))
            ).alias("failed_transaction_count"),
        )
        .withColumn(
            "payment_failure_rate",
            F.when(
                F.col("transaction_count") > 0,
                F.col("failed_transaction_count") / F.col("transaction_count"),
            ),
        )
        .withColumn(
            "_dimension_key",
            F.concat_ws("::", F.col("payment_date").cast("string"), F.col("payment_method")),
        )
    )
    specs.append(
        (
            payment_by_method,
            "payment_method_failure_rate",
            F.col("_dimension_key"),
            F.col("payment_failure_rate"),
            "ratio",
        )
    )

    frames = [
        _metric_observations(
            frame,
            run_id=run_id,
            pipeline_run_id=pipeline_run_id,
            recorded_at_utc=recorded_at_utc,
            metric_name=metric_name,
            dimension_expression=dimension_expression,
            metric_expression=metric_expression,
            metric_unit=metric_unit,
        )
        for frame, metric_name, dimension_expression, metric_expression, metric_unit in specs
    ]

    observations = frames[0]
    for frame in frames[1:]:
        observations = observations.unionByName(frame)
    return observations


def evaluate_quality_anomalies(
    current_observations: DataFrame,
    historical_observations: DataFrame,
    *,
    min_history_observations: int = 5,
    z_score_threshold: float = 3.0,
    relative_change_threshold: float = 0.50,
    absolute_floor: float = 0.05,
) -> DataFrame:
    """Compare this run with prior non-anomalous observations for the same key.

    A metric is flagged only after enough history exists and its absolute
    deviation exceeds both the configured standard-deviation and relative
    change tolerances. Anomalies are advisory; hard data-quality gates remain
    in build_gold_outputs/validate_gold_outputs.
    """
    if min_history_observations < 1:
        raise ValueError("min_history_observations must be at least 1.")
    if z_score_threshold <= 0:
        raise ValueError("z_score_threshold must be greater than zero.")
    if relative_change_threshold <= 0:
        raise ValueError("relative_change_threshold must be greater than zero.")
    if absolute_floor < 0:
        raise ValueError("absolute_floor must not be negative.")

    _require_columns(current_observations, OBSERVATION_COLUMNS, "current_observations")
    _require_columns(
        historical_observations,
        ("metric_name", "dimension_key", "metric_unit", "metric_value", "status"),
        "historical_observations",
    )

    baseline = (
        historical_observations
        .filter(F.col("status").isin("NORMAL", "INSUFFICIENT_HISTORY"))
        .groupBy("metric_name", "dimension_key", "metric_unit")
        .agg(
            F.count("metric_value").cast("long").alias("baseline_observation_count"),
            F.avg("metric_value").alias("baseline_mean"),
            F.stddev_pop("metric_value").alias("baseline_stddev"),
        )
    )

    compared = (
        current_observations
        .join(baseline, on=["metric_name", "dimension_key", "metric_unit"], how="left")
        .withColumn(
            "baseline_observation_count",
            F.coalesce(F.col("baseline_observation_count"), F.lit(0)).cast("long"),
        )
        .withColumn(
            "_absolute_deviation",
            F.abs(F.col("metric_value") - F.col("baseline_mean")),
        )
        .withColumn(
            "_baseline_scale",
            F.greatest(F.abs(F.coalesce(F.col("baseline_mean"), F.lit(0.0))), F.lit(absolute_floor)),
        )
        .withColumn(
            "_anomaly_tolerance",
            F.greatest(
                F.coalesce(F.col("baseline_stddev"), F.lit(0.0)) * F.lit(z_score_threshold),
                F.col("_baseline_scale") * F.lit(relative_change_threshold),
            ),
        )
        .withColumn(
            "deviation_ratio",
            F.when(
                F.col("baseline_mean").isNotNull(),
                F.col("_absolute_deviation") / F.col("_baseline_scale"),
            ).cast("double"),
        )
        .withColumn(
            "deviation_z_score",
            F.when(
                F.col("baseline_stddev") > 0,
                F.col("_absolute_deviation") / F.col("baseline_stddev"),
            ).cast("double"),
        )
        .withColumn(
            "status",
            F.when(
                F.col("baseline_observation_count") < min_history_observations,
                F.lit("INSUFFICIENT_HISTORY"),
            )
            .when(F.col("_absolute_deviation") > F.col("_anomaly_tolerance"), F.lit("ANOMALY"))
            .otherwise(F.lit("NORMAL")),
        )
    )

    return compared.select(
        F.col("run_id").cast("string").alias("run_id"),
        F.col("pipeline_run_id").cast("string").alias("pipeline_run_id"),
        F.col("recorded_at_utc").cast("timestamp").alias("recorded_at_utc"),
        F.col("metric_name").cast("string").alias("metric_name"),
        F.col("dimension_key").cast("string").alias("dimension_key"),
        F.col("metric_unit").cast("string").alias("metric_unit"),
        F.col("metric_value").cast("double").alias("metric_value"),
        F.col("baseline_mean").cast("double").alias("baseline_mean"),
        F.col("baseline_stddev").cast("double").alias("baseline_stddev"),
        F.col("baseline_observation_count").cast("long").alias("baseline_observation_count"),
        F.col("deviation_ratio").cast("double").alias("deviation_ratio"),
        F.col("deviation_z_score").cast("double").alias("deviation_z_score"),
        F.col("status").cast("string").alias("status"),
    )
