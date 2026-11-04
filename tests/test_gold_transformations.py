"""Local Spark tests for the Gold transformation and publication contract.

These tests execute PySpark transformations with small synthetic in-memory inputs.
They do not call Azure Data Factory, ADLS, or a Databricks workspace.
"""
import unittest

from pyspark.sql import SparkSession, functions as F

from src.gold.transformations import build_gold_outputs, validate_gold_outputs


class GoldTransformationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("uber-eats-gold-unit-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.silver = {
            "deliveries": self.spark.createDataFrame([
                (1001, 501, 41, "2026-09-01 12:10:00", "2026-09-01 12:55:00", "DELIVERED", "2026-09-01 12:55:00"),
                (1002, 502, 41, None, None, "CANCELLED", "2026-09-01 13:30:00"),
            ], ["delivery_id", "order_id", "driver_id", "pickup_time", "delivery_time", "delivery_status", "updated_at"]),
            "payments": self.spark.createDataFrame([
                (9001, 501, "CARD", 450.0, "SUCCESS", "2026-09-01 12:05:00"),
                (9002, 502, "UPI", 200.0, "FAILED", "2026-09-01 13:05:00"),
            ], ["payment_id", "order_id", "payment_method", "amount", "payment_status", "payment_timestamp"]),
            "order_status": self.spark.createDataFrame([
                (501, "PLACED", "2026-09-01 12:00:00"),
                (501, "DELIVERED", "2026-09-01 12:55:00"),
                (502, "PLACED", "2026-09-01 13:00:00"),
                (502, "CANCELLED", "2026-09-01 13:30:00"),
            ], ["order_id", "status", "status_timestamp"]),
            "ratings": self.spark.createDataFrame([
                (1101, 501, 31, 201, 5.0, "Excellent", "2026-09-01 14:00:00"),
            ], ["rating_id", "order_id", "customer_id", "restaurant_id", "rating", "review", "created_at"]),
            "drivers": self.spark.createDataFrame([
                (41, "Sample Driver", "+910000000041", "BIKE", "Hyderabad", "2025-01-10", "ACTIVE"),
            ], ["driver_id", "driver_name", "phone", "vehicle_type", "city", "join_date", "status"]),
        }

    def test_builds_all_gold_outputs_and_reconciles_documented_kpis(self):
        outputs = build_gold_outputs(self.silver)
        self.assertEqual({
            "gold_order_360",
            "gold_daily_order_performance",
            "gold_payment_method_performance",
            "gold_restaurant_feedback",
            "gold_driver_performance",
        }, set(outputs))

        order_rows = outputs["gold_order_360"].collect()
        self.assertEqual(2, len(order_rows))
        orders = {row["order_id"]: row.asDict() for row in order_rows}
        self.assertEqual("DELIVERED", orders["501"]["current_order_status"])
        self.assertEqual("PAID", orders["501"]["payment_status"])
        self.assertEqual(450.0, orders["501"]["successful_payment_amount"])
        self.assertEqual(45.0, orders["501"]["delivery_minutes"])
        self.assertEqual("CANCELLED", orders["502"]["current_order_status"])
        self.assertEqual("FAILED", orders["502"]["payment_status"])

        daily = outputs["gold_daily_order_performance"].collect()
        self.assertEqual(1, len(daily))
        self.assertEqual(2, daily[0]["order_count"])
        self.assertEqual(1, daily[0]["delivered_orders"])
        self.assertEqual(1, daily[0]["cancelled_orders"])
        self.assertEqual(1, daily[0]["paid_orders"])
        self.assertEqual(450.0, daily[0]["successful_payment_amount"])

        driver = outputs["gold_driver_performance"].first()
        self.assertEqual(2, driver["delivery_record_count"])
        self.assertEqual(1, driver["delivered_record_count"])
        self.assertAlmostEqual(0.5, driver["delivery_completion_rate"])

        restaurant = outputs["gold_restaurant_feedback"].first()
        self.assertEqual("201", restaurant["restaurant_id"])
        self.assertEqual(5.0, restaurant["average_rating"])

    def test_rejects_duplicate_payment_business_keys(self):
        self.silver["payments"] = self.spark.createDataFrame([
            (9001, 501, "CARD", 100.0, "SUCCESS", "2026-09-01 12:05:00"),
            (9001, 502, "UPI", 200.0, "FAILED", "2026-09-01 13:05:00"),
        ], ["payment_id", "order_id", "payment_method", "amount", "payment_status", "payment_timestamp"])
        with self.assertRaisesRegex(ValueError, "duplicate key groups"):
            build_gold_outputs(self.silver)

    def test_rejects_missing_required_source_columns(self):
        self.silver["drivers"] = self.spark.createDataFrame([(41, "Sample Driver")], ["driver_id", "driver_name"])
        with self.assertRaisesRegex(ValueError, "missing required columns"):
            build_gold_outputs(self.silver)

    def test_rejects_invalid_rating_range(self):
        self.silver["ratings"] = self.spark.createDataFrame([
            (1101, 501, 31, 201, 8.0, "Out of range", "2026-09-01 14:00:00"),
        ], ["rating_id", "order_id", "customer_id", "restaurant_id", "rating", "review", "created_at"])
        with self.assertRaisesRegex(ValueError, "out-of-range ratings"):
            build_gold_outputs(self.silver)

    def test_rejects_blank_payment_business_status(self):
        self.silver["payments"] = self.silver["payments"].withColumn(
            "payment_status",
            F.when(F.col("payment_id") == 9001, F.lit("  ")).otherwise(F.col("payment_status")),
        )
        with self.assertRaisesRegex(ValueError, "blank values"):
            build_gold_outputs(self.silver)

    def test_rejects_unparseable_payment_timestamp(self):
        self.silver["payments"] = self.silver["payments"].withColumn(
            "payment_timestamp",
            F.when(F.col("payment_id") == 9001, F.lit("not-a-timestamp")).otherwise(F.col("payment_timestamp")),
        )
        with self.assertRaisesRegex(ValueError, "unparseable payment_timestamp"):
            build_gold_outputs(self.silver)

    def test_rejects_delivery_duration_with_negative_elapsed_time(self):
        self.silver["deliveries"] = self.silver["deliveries"].withColumn(
            "delivery_time",
            F.when(F.col("delivery_id") == 1001, F.lit("2026-09-01 11:55:00")).otherwise(F.col("delivery_time")),
        )
        with self.assertRaisesRegex(ValueError, "earlier than pickup"):
            build_gold_outputs(self.silver)

    def test_rejects_delivery_referencing_unknown_driver(self):
        self.silver["deliveries"] = self.silver["deliveries"].withColumn(
            "driver_id",
            F.when(F.col("delivery_id") == 1001, F.lit(999)).otherwise(F.col("driver_id")),
        )
        with self.assertRaisesRegex(ValueError, "absent from the drivers dimension"):
            build_gold_outputs(self.silver)

    def test_rejects_conflicting_customer_or_restaurant_for_one_order(self):
        extra_rating = self.spark.createDataFrame(
            [(1102, 501, 31, 999, 4.0, "Different restaurant", "2026-09-01 15:00:00")],
            ["rating_id", "order_id", "customer_id", "restaurant_id", "rating", "review", "created_at"],
        )
        self.silver["ratings"] = self.silver["ratings"].unionByName(extra_rating)
        with self.assertRaisesRegex(ValueError, "more than one customer or restaurant"):
            build_gold_outputs(self.silver)

    def test_output_gate_rejects_duplicate_daily_grain(self):
        outputs = build_gold_outputs(self.silver)
        outputs["gold_daily_order_performance"] = outputs["gold_daily_order_performance"].unionByName(
            outputs["gold_daily_order_performance"]
        )
        with self.assertRaisesRegex(ValueError, "duplicate key groups"):
            validate_gold_outputs(outputs)

    def test_output_gate_rejects_inconsistent_daily_kpis(self):
        outputs = build_gold_outputs(self.silver)
        outputs["gold_daily_order_performance"] = outputs["gold_daily_order_performance"].withColumn(
            "paid_orders", F.lit(99)
        )
        with self.assertRaisesRegex(ValueError, "daily KPI bounds"):
            validate_gold_outputs(outputs)

    def test_output_gate_rejects_cross_table_payment_amount_mismatch(self):
        outputs = build_gold_outputs(self.silver)
        outputs["gold_payment_method_performance"] = (
            outputs["gold_payment_method_performance"]
            .withColumn("successful_payment_amount", F.col("successful_payment_amount") + F.lit(10.0))
            .withColumn("gross_attempt_amount", F.col("gross_attempt_amount") + F.lit(10.0))
        )
        with self.assertRaisesRegex(ValueError, "does not reconcile across"):
            validate_gold_outputs(outputs)


if __name__ == "__main__":
    unittest.main()
