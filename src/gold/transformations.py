"""Validated Spark transformations from Silver operational data to Gold analytics tables.

Only fields available in the current Silver contracts are used. Promotions and
zones are not joined to orders because no reliable promo_id or zone_id foreign
keys are present in the current order/delivery model.
"""
from __future__ import annotations

from functools import reduce
from math import isclose
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
        raise ValueError(f"{dataset} input is missing required columns: {missing}")


def _assert_non_null(frame: DataFrame, columns: tuple[str, ...], dataset: str) -> None:
    condition = reduce(or_, (F.col(name).isNull() for name in columns))
    if frame.filter(condition).limit(1).count():
        raise ValueError(f"{dataset} contains null values in required fields: {list(columns)}")


def _assert_non_blank(frame: DataFrame, columns: tuple[str, ...], dataset: str) -> None:
    condition = reduce(
        or_,
        (
            F.col(name).isNotNull()
            & (F.length(F.trim(F.col(name).cast("string"))) == 0)
            for name in columns
        ),
    )
    if frame.filter(condition).limit(1).count():
        raise ValueError(f"{dataset} contains blank values in required fields: {list(columns)}")


def _assert_unique(frame: DataFrame, keys: tuple[str, ...], dataset: str) -> None:
    duplicate = (
        frame.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        raise ValueError(f"{dataset} contains duplicate key groups for {list(keys)}")


def _assert_no_invalid_rows(
    frame: DataFrame,
    condition,
    dataset: str,
    message: str,
) -> None:
    if frame.filter(condition).limit(1).count():
        raise ValueError(f"{dataset} {message}")


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
    _assert_non_null(deliveries, ("delivery_id", "order_id", "updated_at"), "deliveries")
    _assert_non_blank(deliveries, ("delivery_id", "order_id", "delivery_status"), "deliveries")
    invalid_delivery_timestamps = (
        F.to_timestamp("updated_at").isNull()
        | (
            F.col("pickup_time").isNotNull()
            & F.to_timestamp("pickup_time").isNull()
        )
        | (
            F.col("delivery_time").isNotNull()
            & F.to_timestamp("delivery_time").isNull()
        )
    )
    _assert_no_invalid_rows(
        deliveries,
        invalid_delivery_timestamps,
        "deliveries",
        "contains null or unparseable required/available timestamps",
    )
    _assert_unique(deliveries, ("delivery_id",), "deliveries")
    deliveries = (
        deliveries
        .withColumn("delivery_id", F.col("delivery_id").cast("string"))
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("driver_id", F.col("driver_id").cast("string"))
        .withColumn("pickup_time", F.to_timestamp("pickup_time"))
        .withColumn("delivery_time", F.to_timestamp("delivery_time"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("delivery_status", F.upper(F.trim(F.col("delivery_status").cast("string"))))
    )
    _assert_no_invalid_rows(
        deliveries,
        F.col("pickup_time").isNotNull()
        & F.col("delivery_time").isNotNull()
        & (F.col("delivery_time") < F.col("pickup_time")),
        "deliveries",
        "contains delivery timestamps earlier than pickup timestamps",
    )
    _assert_no_invalid_rows(
        deliveries,
        (F.col("delivery_status") == "DELIVERED")
        & (F.col("pickup_time").isNull() | F.col("delivery_time").isNull()),
        "deliveries",
        "marks a delivery DELIVERED without both pickup_time and delivery_time",
    )

    payments = silver["payments"]
    _require_columns(
        payments,
        ("payment_id", "order_id", "payment_method", "amount", "payment_status", "payment_timestamp"),
        "payments",
    )
    _assert_non_null(
        payments,
        ("payment_id", "order_id", "payment_method", "amount", "payment_status", "payment_timestamp"),
        "payments",
    )
    _assert_non_blank(payments, ("payment_id", "order_id", "payment_method", "payment_status"), "payments")
    _assert_unique(payments, ("payment_id",), "payments")
    _assert_no_invalid_rows(
        payments,
        F.to_timestamp("payment_timestamp").isNull(),
        "payments",
        "contains null or unparseable payment_timestamp values",
    )
    payments = (
        payments
        .withColumn("payment_id", F.col("payment_id").cast("string"))
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("payment_timestamp", F.to_timestamp("payment_timestamp"))
        .withColumn("payment_method", F.upper(F.trim(F.col("payment_method").cast("string"))))
        .withColumn("payment_status", F.upper(F.trim(F.col("payment_status").cast("string"))))
    )
    invalid_amount = payments.filter(
        F.col("amount").isNull()
        | F.isnan("amount")
        | (F.col("amount") < 0)
        | (F.abs(F.col("amount")) == F.lit(float("inf")))
    ).limit(1).count()
    if invalid_amount:
        raise ValueError("payments input contains null, nonnumeric, NaN, infinite, or negative amounts")

    order_status = silver["order_status"]
    _require_columns(order_status, ("order_id", "status", "status_timestamp"), "order_status")
    _assert_non_null(order_status, ("order_id", "status", "status_timestamp"), "order_status")
    _assert_non_blank(order_status, ("order_id", "status"), "order_status")
    _assert_no_invalid_rows(
        order_status,
        F.to_timestamp("status_timestamp").isNull(),
        "order_status",
        "contains unparseable status_timestamp values",
    )
    _assert_unique(order_status, ("order_id", "status_timestamp"), "order_status")
    order_status = (
        order_status
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("status", F.upper(F.trim(F.col("status").cast("string"))))
        .withColumn("status_timestamp", F.to_timestamp("status_timestamp"))
    )

    ratings = silver["ratings"]
    _require_columns(
        ratings,
        ("rating_id", "order_id", "customer_id", "restaurant_id", "rating", "created_at"),
        "ratings",
    )
    _assert_non_null(
        ratings,
        ("rating_id", "order_id", "customer_id", "restaurant_id", "rating", "created_at"),
        "ratings",
    )
    _assert_non_blank(ratings, ("rating_id", "order_id", "customer_id", "restaurant_id"), "ratings")
    _assert_unique(ratings, ("rating_id",), "ratings")
    _assert_no_invalid_rows(
        ratings,
        F.to_timestamp("created_at").isNull(),
        "ratings",
        "contains unparseable created_at values",
    )
    ratings = (
        ratings
        .withColumn("rating_id", F.col("rating_id").cast("string"))
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
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
        raise ValueError("ratings input contains null, nonnumeric, NaN, or out-of-range ratings")

    conflicting_rating_parents = (
        ratings.groupBy("order_id")
        .agg(
            F.countDistinct("customer_id").alias("customer_count"),
            F.countDistinct("restaurant_id").alias("restaurant_count"),
        )
        .filter((F.col("customer_count") > 1) | (F.col("restaurant_count") > 1))
        .limit(1)
        .count()
    )
    if conflicting_rating_parents:
        raise ValueError(
            "ratings input assigns more than one customer or restaurant to the same order"
        )

    drivers = silver["drivers"]
    _require_columns(
        drivers,
        ("driver_id", "driver_name", "vehicle_type", "city", "status", "join_date"),
        "drivers",
    )
    _assert_non_null(drivers, ("driver_id",), "drivers")
    _assert_non_blank(drivers, ("driver_id",), "drivers")
    _assert_unique(drivers, ("driver_id",), "drivers")
    drivers = (
        drivers
        .withColumn("driver_id", F.col("driver_id").cast("string"))
        .withColumn("vehicle_type", F.upper(F.trim(F.col("vehicle_type").cast("string"))))
        .withColumn("status", F.upper(F.trim(F.col("status").cast("string"))))
    )

    referenced_driver_ids = (
        deliveries
        .filter(F.col("driver_id").isNotNull())
        .select("driver_id")
        .distinct()
    )
    unknown_driver = (
        referenced_driver_ids
        .join(drivers.select("driver_id").distinct(), on="driver_id", how="left_anti")
        .limit(1)
        .count()
    )
    if unknown_driver:
        raise ValueError("deliveries input references driver_id values absent from the drivers dimension")

    return {
        "deliveries": deliveries,
        "payments": payments,
        "order_status": order_status,
        "ratings": ratings,
        "drivers": drivers,
    }


def validate_gold_outputs(outputs: dict[str, DataFrame]) -> None:
    """Enforce Gold output grains, KPI bounds, and cross-table reconciliation."""
    grains = {
        "gold_order_360": (("order_id",), ("order_id", "current_order_status", "payment_status")),
        "gold_daily_order_performance": (("order_date",), ("order_date",)),
        "gold_payment_method_performance": (
            ("payment_date", "payment_method", "payment_status"),
            ("payment_date", "payment_method", "payment_status"),
        ),
        "gold_restaurant_feedback": (("restaurant_id",), ("restaurant_id",)),
        "gold_driver_performance": (("driver_id",), ("driver_id",)),
    }
    missing_outputs = sorted(set(grains) - set(outputs))
    if missing_outputs:
        raise ValueError(f"Gold output set is missing required datasets: {missing_outputs}")

    for dataset, (keys, required_columns) in grains.items():
        _require_columns(outputs[dataset], required_columns, dataset)
        _assert_non_null(outputs[dataset], keys, dataset)
        _assert_non_blank(outputs[dataset], keys, dataset)
        _assert_unique(outputs[dataset], keys, dataset)

    order_360 = outputs["gold_order_360"]
    _assert_no_invalid_rows(
        order_360,
        (F.col("payment_transaction_count") < 0)
        | (F.col("successful_payment_count") < 0)
        | (F.col("failed_payment_count") < 0)
        | (F.col("successful_payment_count") > F.col("payment_transaction_count"))
        | (F.col("failed_payment_count") > F.col("payment_transaction_count"))
        | (F.col("successful_payment_amount") < 0)
        | (F.col("payment_attempt_amount") < F.col("successful_payment_amount")),
        "gold_order_360",
        "violates payment count or amount invariants",
    )

    daily = outputs["gold_daily_order_performance"]
    _require_columns(
        daily,
        (
            "order_count", "delivered_orders", "cancelled_orders", "active_orders",
            "paid_orders", "successful_payment_amount", "average_delivery_minutes",
            "average_customer_rating",
        ),
        "gold_daily_order_performance",
    )
    _assert_no_invalid_rows(
        daily,
        (F.col("order_count") <= 0)
        | (F.col("delivered_orders") < 0)
        | (F.col("cancelled_orders") < 0)
        | (F.col("active_orders") < 0)
        | (F.col("paid_orders") < 0)
        | (F.col("paid_orders") > F.col("order_count"))
        | (
            F.col("delivered_orders")
            + F.col("cancelled_orders")
            + F.col("active_orders")
            > F.col("order_count")
        )
        | (F.col("successful_payment_amount") < 0)
        | (F.col("average_delivery_minutes") < 0)
        | (
            F.col("average_customer_rating").isNotNull()
            & (
                (F.col("average_customer_rating") < 1)
                | (F.col("average_customer_rating") > 5)
            )
        ),
        "gold_daily_order_performance",
        "violates daily KPI bounds or mutually exclusive order-state counts",
    )

    payment = outputs["gold_payment_method_performance"]
    _require_columns(
        payment,
        (
            "transaction_count", "distinct_order_count", "gross_attempt_amount",
            "successful_payment_amount",
        ),
        "gold_payment_method_performance",
    )
    _assert_no_invalid_rows(
        payment,
        (F.col("transaction_count") <= 0)
        | (F.col("distinct_order_count") <= 0)
        | (F.col("distinct_order_count") > F.col("transaction_count"))
        | (F.col("gross_attempt_amount") < 0)
        | (F.col("successful_payment_amount") < 0)
        | (F.col("successful_payment_amount") > F.col("gross_attempt_amount")),
        "gold_payment_method_performance",
        "violates transaction-count or payment-amount invariants",
    )

    restaurant = outputs["gold_restaurant_feedback"]
    _require_columns(
        restaurant,
        (
            "rated_order_count", "rating_count", "average_rating",
            "five_star_rating_count", "five_star_rating_share",
        ),
        "gold_restaurant_feedback",
    )
    _assert_no_invalid_rows(
        restaurant,
        (F.col("rated_order_count") <= 0)
        | (F.col("rating_count") < F.col("rated_order_count"))
        | (F.col("average_rating") < 1)
        | (F.col("average_rating") > 5)
        | (F.col("five_star_rating_count") < 0)
        | (F.col("five_star_rating_count") > F.col("rating_count"))
        | (F.col("five_star_rating_share") < 0)
        | (F.col("five_star_rating_share") > 1),
        "gold_restaurant_feedback",
        "violates rating-count, average-rating, or five-star-share invariants",
    )

    driver = outputs["gold_driver_performance"]
    _require_columns(
        driver,
        (
            "delivery_record_count", "distinct_order_count", "delivered_record_count",
            "cancelled_record_count", "average_delivery_minutes", "delivery_completion_rate",
        ),
        "gold_driver_performance",
    )
    _assert_no_invalid_rows(
        driver,
        (F.col("delivery_record_count") <= 0)
        | (F.col("distinct_order_count") <= 0)
        | (F.col("distinct_order_count") > F.col("delivery_record_count"))
        | (F.col("delivered_record_count") < 0)
        | (F.col("cancelled_record_count") < 0)
        | (
            F.col("delivered_record_count") + F.col("cancelled_record_count")
            > F.col("delivery_record_count")
        )
        | (F.col("average_delivery_minutes") < 0)
        | (
            F.col("delivery_completion_rate").isNotNull()
            & (
                (F.col("delivery_completion_rate") < 0)
                | (F.col("delivery_completion_rate") > 1)
            )
        ),
        "gold_driver_performance",
        "violates delivery-count, duration, or completion-rate invariants",
    )

    # Every order with a usable order_date must appear once in the daily aggregate.
    expected_daily_orders = order_360.filter(F.col("order_date").isNotNull()).count()
    actual_daily_orders = (
        daily.agg(F.coalesce(F.sum("order_count"), F.lit(0)).alias("total")).first()["total"]
        or 0
    )
    if int(expected_daily_orders) != int(actual_daily_orders):
        raise ValueError(
            "Gold daily order_count does not reconcile to gold_order_360 rows with an order_date"
        )

    # Successful amount is independently derived by order and by payment transaction.
    order_success_amount = (
        order_360.agg(
            F.coalesce(F.sum("successful_payment_amount"), F.lit(0.0)).alias("total")
        ).first()["total"]
        or 0.0
    )
    daily_success_amount = (
        daily.agg(
            F.coalesce(F.sum("successful_payment_amount"), F.lit(0.0)).alias("total")
        ).first()["total"]
        or 0.0
    )
    payment_success_amount = (
        payment.agg(
            F.coalesce(F.sum("successful_payment_amount"), F.lit(0.0)).alias("total")
        ).first()["total"]
        or 0.0
    )
    if not (
        isclose(float(order_success_amount), float(daily_success_amount), rel_tol=1e-9, abs_tol=1e-6)
        and isclose(float(order_success_amount), float(payment_success_amount), rel_tol=1e-9, abs_tol=1e-6)
    ):
        raise ValueError(
            "Gold successful_payment_amount does not reconcile across order, daily, and payment outputs"
        )


def build_gold_outputs(silver: dict[str, DataFrame]) -> dict[str, DataFrame]:
    """Build five Gold tables, validate their grains, and reconcile core KPIs."""
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
        F.min("customer_id").alias("customer_id"),
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
        .filter(F.col("driver_id").isNotNull())
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

    outputs = {
        "gold_order_360": order_360,
        "gold_daily_order_performance": daily_order_performance,
        "gold_payment_method_performance": payment_method_performance,
        "gold_restaurant_feedback": restaurant_feedback,
        "gold_driver_performance": driver_performance,
    }
    validate_gold_outputs(outputs)
    return outputs
