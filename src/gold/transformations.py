"""Validated Spark transformations from Silver operational data to Gold analytics tables.

Only fields available in the current Silver contracts are used. Promotions and
zones are not joined to orders because no reliable promo_id or zone_id foreign
keys are present in the current order/delivery model.
"""
from __future__ import annotations

from functools import reduce
from operator import or_

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


SUCCESS_PAYMENT_STATUSES = ("SUCCESS", "COMPLETED")
FAILED_PAYMENT_STATUSES = ("FAILED", "DECLINED", "ERROR")
DELIVERED_STATUSES = ("DELIVERED",)
CANCELLED_STATUSES = ("CANCELLED",)
ACTIVE_ORDER_STATUSES = ("PLACED", "PREPARING", "ASSIGNED", "OUT_FOR_DELIVERY")


def _require_columns(frame: DataFrame, required: tuple[str, ...], dataset: str) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise ValueError(f"{dataset} Silver input is missing required columns: {missing}")


def _assert_non_null(frame: DataFrame, columns: tuple[str, ...], dataset: str) -> None:
    condition = reduce(or_, (F.col(name).isNull() for name in columns))
    if frame.filter(condition).limit(1).count():
        raise ValueError(f"{dataset} Silver input contains null values in required keys: {list(columns)}")


def _assert_unique(frame: DataFrame, keys: tuple[str, ...], dataset: str) -> None:
    duplicate = (
        frame.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        raise ValueError(f"{dataset} Silver input contains duplicate key groups for {list(keys)}")


def _prepare_inputs(silver: dict[str, DataFrame]) -> dict[str, DataFrame]:
    required_sources = {"deliveries", "payments", "order_status", "ratings", "drivers"}
    missing_sources = sorted(required_sources - set(silver))
    if missing_sources:
        raise ValueError(f"Missing required Silver DataFrames: {missing_sources}")

    deliveries = silver["deliveries"]
    _require_columns(
        deliveries,
        ("delivery_id", "order_id", "driver_id", "pickup_time", "delivery_time", "delivery_status", "updated_at"),
        "deliveries",
    )
    _assert_non_null(deliveries, ("delivery_id", "order_id"), "deliveries")
    _assert_unique(deliveries, ("delivery_id",), "deliveries")
    deliveries = (
        deliveries
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("driver_id", F.col("driver_id").cast("string"))
        .withColumn("pickup_time", F.to_timestamp("pickup_time"))
        .withColumn("delivery_time", F.to_timestamp("delivery_time"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("delivery_status", F.upper(F.trim(F.col("delivery_status").cast("string"))))
    )

    payments = silver["payments"]
    _require_columns(
        payments,
        ("payment_id", "order_id", "payment_method", "amount", "payment_status", "payment_timestamp"),
        "payments",
    )
    _assert_non_null(payments, ("payment_id", "order_id"), "payments")
    _assert_unique(payments, ("payment_id",), "payments")
    payments = (
        payments
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("payment_timestamp", F.to_timestamp("payment_timestamp"))
        .withColumn("payment_method", F.upper(F.trim(F.col("payment_method").cast("string"))))
        .withColumn("payment_status", F.upper(F.trim(F.col("payment_status").cast("string"))))
    )
    invalid_amount = payments.filter(
        F.col("amount").isNull() | F.isnan("amount") | (F.col("amount") < 0)
    ).limit(1).count()
    if invalid_amount:
        raise ValueError("payments Silver input contains null, nonnumeric, NaN, or negative amounts")

    order_status = silver["order_status"]
    _require_columns(order_status, ("order_id", "status", "status_timestamp"), "order_status")
    _assert_non_null(order_status, ("order_id", "status", "status_timestamp"), "order_status")
    _assert_unique(order_status, ("order_id", "status_timestamp"), "order_status")
    order_status = (
        order_status
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("status", F.upper(F.trim(F.col("status").cast("string"))))
        .withColumn("status_timestamp", F.to_timestamp("status_timestamp"))
    )
    if order_status.filter(F.col("status_timestamp").isNull()).limit(1).count():
        raise ValueError("order_status Silver input contains unparseable status_timestamp values")

    ratings = silver["ratings"]
    _require_columns(
        ratings,
        ("rating_id", "order_id", "customer_id", "restaurant_id", "rating", "created_at"),
        "ratings",
    )
    _assert_non_null(ratings, ("rating_id", "order_id", "restaurant_id"), "ratings")
    _assert_unique(ratings, ("rating_id",), "ratings")
    ratings = (
        ratings
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("restaurant_id", F.col("restaurant_id").cast("string"))
        .withColumn("rating", F.col("rating").cast("double"))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )
    invalid_rating = ratings.filter(
        F.col("rating").isNull()
        | F.isnan("rating")
        | (F.col("rating") < 1)
        | (F.col("rating") > 5)
    ).limit(1).count()
    if invalid_rating:
        raise ValueError("ratings Silver input contains null, nonnumeric, NaN, or out-of-range ratings")

    drivers = silver["drivers"]
    _require_columns(
        drivers,
        ("driver_id", "driver_name", "vehicle_type", "city", "status", "join_date"),
        "drivers",
    )
    _assert_non_null(drivers, ("driver_id",), "drivers")
    _assert_unique(drivers, ("driver_id",), "drivers")
    drivers = (
        drivers
        .withColumn("driver_id", F.col("driver_id").cast("string"))
        .withColumn("vehicle_type", F.upper(F.trim(F.col("vehicle_type").cast("string"))))
        .withColumn("status", F.upper(F.trim(F.col("status").cast("string"))))
    )

    return {
        "deliveries": deliveries,
        "payments": payments,
        "order_status": order_status,
        "ratings": ratings,
        "drivers": drivers,
    }


def build_gold_outputs(silver: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Build five Gold tables with explicit grains and stable business definitions."""
    prepared = _prepare_inputs(silver)
    deliveries = prepared["deliveries"]
    payments = prepared["payments"]
    status_events = prepared["order_status"]
    ratings = prepared["ratings"]
    drivers = prepared["drivers"]

    status_created = status_events.groupBy("order_id").agg(
        F.min("status_timestamp").alias("event_created_at")
    )
    status_window = Window.partitionBy("order_id").orderBy(
        F.col("status_timestamp").desc(), F.col("status").desc()
    )
    latest_status = (
        status_events
        .withColumn("_row_number", F.row_number().over(status_window))
        .filter(F.col("_row_number") == 1)
        .select(
            "order_id",
            F.col("status").alias("latest_event_status"),
            F.col("status_timestamp").alias("latest_status_at"),
        )
    )

    delivery_window = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc_nulls_last(), F.col("delivery_id").cast("string").desc()
    )
    latest_delivery = (
        deliveries
        .withColumn("_row_number", F.row_number().over(delivery_window))
        .filter(F.col("_row_number") == 1)
        .withColumn(
            "delivery_minutes",
            F.when(
                F.col("pickup_time").isNotNull()
                & F.col("delivery_time").isNotNull()
                & (F.col("delivery_time") >= F.col("pickup_time")),
                (F.unix_timestamp("delivery_time") - F.unix_timestamp("pickup_time")) / F.lit(60.0),
            ),
        )
        .select(
            "order_id",
            F.col("delivery_id").cast("string").alias("latest_delivery_id"),
            F.col("driver_id").alias("latest_driver_id"),
            F.col("delivery_status").alias("latest_delivery_status"),
            F.col("pickup_time").alias("latest_pickup_at"),
            F.col("delivery_time").alias("latest_delivery_at"),
            "delivery_minutes",
        )
    )

    payment_facts = payments.withColumn(
        "_is_success", F.col("payment_status").isin(*SUCCESS_PAYMENT_STATUSES)
    ).withColumn(
        "_is_failed", F.col("payment_status").isin(*FAILED_PAYMENT_STATUSES)
    )
    payment_by_order = payment_facts.groupBy("order_id").agg(
        F.count("*").alias("payment_transaction_count"),
        F.sum(F.when(F.col("_is_success"), 1).otherwise(0)).cast("long").alias("successful_payment_count"),
        F.sum(F.when(F.col("_is_failed"), 1).otherwise(0)).cast("long").alias("failed_payment_count"),
        F.sum(F.when(F.col("_is_success"), F.col("amount")).otherwise(F.lit(0.0))).alias("successful_payment_amount"),
        F.sum("amount").alias("payment_attempt_amount"),
        F.min("payment_timestamp").alias("first_payment_at"),
        F.max("payment_timestamp").alias("last_payment_at"),
    ).withColumn(
        "derived_payment_status",
        F.when(F.col("successful_payment_count") > 0, F.lit("PAID"))
        .when(F.col("failed_payment_count") > 0, F.lit("FAILED"))
        .otherwise(F.lit("PENDING")),
    )

    rating_by_order = ratings.groupBy("order_id").agg(
        F.avg("rating").alias("average_customer_rating"),
        F.count("*").alias("rating_count"),
        F.min("restaurant_id").alias("restaurant_id"),
        F.min("customer_id").cast("string").alias("customer_id"),
        F.min("created_at").alias("first_rating_at"),
    )

    all_order_ids = (
        status_events.select("order_id")
        .unionByName(deliveries.select("order_id"))
        .unionByName(payments.select("order_id"))
        .unionByName(ratings.select("order_id"))
        .distinct()
    )
    order_360 = (
        all_order_ids
        .join(status_created, "order_id", "left")
        .join(latest_status, "order_id", "left")
        .join(latest_delivery, "order_id", "left")
        .join(payment_by_order, "order_id", "left")
        .join(rating_by_order, "order_id", "left")
        .withColumn(
            "order_created_at",
            F.coalesce("event_created_at", "latest_pickup_at", "first_payment_at", "first_rating_at"),
        )
        .withColumn(
            "current_order_status",
            F.coalesce("latest_event_status", "latest_delivery_status", F.lit("UNKNOWN")),
        )
        .withColumn("payment_status", F.coalesce("derived_payment_status", F.lit("NO_PAYMENT")))
        .withColumn("order_date", F.to_date("order_created_at"))
        .withColumn("is_delivered", F.col("current_order_status").isin(*DELIVERED_STATUSES))
        .withColumn("is_cancelled", F.col("current_order_status").isin(*CANCELLED_STATUSES))
        .withColumn("is_active", F.col("current_order_status").isin(*ACTIVE_ORDER_STATUSES))
        .withColumn("payment_transaction_count", F.coalesce("payment_transaction_count", F.lit(0)))
        .withColumn("successful_payment_count", F.coalesce("successful_payment_count", F.lit(0)))
        .withColumn("failed_payment_count", F.coalesce("failed_payment_count", F.lit(0)))
        .withColumn("successful_payment_amount", F.coalesce("successful_payment_amount", F.lit(0.0)))
        .withColumn("payment_attempt_amount", F.coalesce("payment_attempt_amount", F.lit(0.0)))
        .withColumn("rating_count", F.coalesce("rating_count", F.lit(0)))
        .drop("event_created_at", "latest_event_status", "derived_payment_status", "first_rating_at")
    )

    daily_order_performance = (
        order_360
        .filter(F.col("order_date").isNotNull())
        .groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum(F.col("is_delivered").cast("int")).alias("delivered_orders"),
            F.sum(F.col("is_cancelled").cast("int")).alias("cancelled_orders"),
            F.sum(F.col("is_active").cast("int")).alias("active_orders"),
            F.sum((F.col("payment_status") == "PAID").cast("int")).alias("paid_orders"),
            F.sum("successful_payment_amount").alias("successful_payment_amount"),
            F.avg("delivery_minutes").alias("average_delivery_minutes"),
            F.avg("average_customer_rating").alias("average_customer_rating"),
        )
    )

    payment_method_performance = (
        payment_facts
        .withColumn("payment_date", F.to_date("payment_timestamp"))
        .groupBy("payment_date", "payment_method", "payment_status")
        .agg(
            F.count("*").alias("transaction_count"),
            F.countDistinct("order_id").alias("distinct_order_count"),
            F.sum("amount").alias("gross_attempt_amount"),
            F.sum(F.when(F.col("_is_success"), F.col("amount")).otherwise(F.lit(0.0))).alias("successful_payment_amount"),
        )
    )

    restaurant_feedback = (
        ratings
        .groupBy("restaurant_id")
        .agg(
            F.countDistinct("order_id").alias("rated_order_count"),
            F.count("*").alias("rating_count"),
            F.avg("rating").alias("average_rating"),
            F.sum((F.col("rating") == 5).cast("int")).alias("five_star_rating_count"),
        )
        .withColumn(
            "five_star_rating_share",
            F.when(F.col("rating_count") > 0, F.col("five_star_rating_count") / F.col("rating_count")),
        )
    )

    delivery_driver_facts = (
        deliveries
        .withColumn(
            "delivery_minutes",
            F.when(
                F.col("pickup_time").isNotNull()
                & F.col("delivery_time").isNotNull()
                & (F.col("delivery_time") >= F.col("pickup_time")),
                (F.unix_timestamp("delivery_time") - F.unix_timestamp("pickup_time")) / F.lit(60.0),
            ),
        )
        .join(drivers, on="driver_id", how="left")
    )
    driver_performance = (
        delivery_driver_facts
        .groupBy("driver_id", "driver_name", "city", "vehicle_type", "status")
        .agg(
            F.count("*").alias("delivery_record_count"),
            F.countDistinct("order_id").alias("distinct_order_count"),
            F.sum(F.col("delivery_status").isin(*DELIVERED_STATUSES).cast("int")).alias("delivered_record_count"),
            F.sum(F.col("delivery_status").isin(*CANCELLED_STATUSES).cast("int")).alias("cancelled_record_count"),
            F.avg("delivery_minutes").alias("average_delivery_minutes"),
        )
        .withColumn(
            "delivery_completion_rate",
            F.when(
                F.col("delivery_record_count") > 0,
                F.col("delivered_record_count") / F.col("delivery_record_count"),
            ),
        )
    )

    return {
        "gold_order_360": order_360,
        "gold_daily_order_performance": daily_order_performance,
        "gold_payment_method_performance": payment_method_performance,
        "gold_restaurant_feedback": restaurant_feedback,
        "gold_driver_performance": driver_performance,
    }
