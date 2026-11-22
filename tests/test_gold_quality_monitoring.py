"""Spark tests for Gold metric observations and historical anomaly detection."""

import unittest
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.gold.quality import (
    build_gold_quality_observations,
    evaluate_quality_anomalies,
    quality_metrics_schema,
)


class GoldQualityMonitoringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("uber-eats-gold-quality-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.gold_outputs = {
            "gold_daily_order_performance": self.spark.createDataFrame(
                [
                    ("2026-11-01", 20, 12, 2, 14, 1250.0, 34.5, 4.4),
                    ("2026-11-02", 25, 15, 3, 18, 1640.0, 39.0, 4.1),
                ],
                [
                    "order_date", "order_count", "delivered_orders", "cancelled_orders",
                    "paid_orders", "successful_payment_amount", "average_delivery_minutes",
                    "average_customer_rating",
                ],
            ),
            "gold_payment_method_performance": self.spark.createDataFrame(
                [
                    ("2026-11-01", "CARD", "SUCCESS", 8),
                    ("2026-11-01", "CARD", "FAILED", 2),
                    ("2026-11-02", "UPI", "SUCCESS", 9),
                    ("2026-11-02", "UPI", "DECLINED", 1),
                ],
                ["payment_date", "payment_method", "payment_status", "transaction_count"],
            ),
            "gold_driver_performance": self.spark.createDataFrame(
                [
                    ("D-101", 18, 0.83, 31.5),
                    ("D-102", 12, 0.75, 42.0),
                ],
                ["driver_id", "delivery_record_count", "delivery_completion_rate", "average_delivery_minutes"],
            ),
            "gold_restaurant_feedback": self.spark.createDataFrame(
                [("R-201", 4.6), ("R-202", 3.9)],
                ["restaurant_id", "average_rating"],
            ),
        }

    def _observations(self, metric_value, dimension_key="2026-11-01", metric_name="daily_order_count"):
        schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("pipeline_run_id", StringType(), False),
            StructField("recorded_at_utc", TimestampType(), False),
            StructField("metric_name", StringType(), False),
            StructField("dimension_key", StringType(), False),
            StructField("metric_unit", StringType(), False),
            StructField("metric_value", DoubleType(), False),
        ])
        return self.spark.createDataFrame(
            [(
                "current-run",
                "adf-run-current",
                datetime(2026, 11, 10, 12, 0, 0),
                metric_name,
                dimension_key,
                "orders",
                float(metric_value),
            )],
            schema=schema,
        )

    def _history(self, values, dimension_key="2026-11-01", metric_name="daily_order_count"):
        schema = quality_metrics_schema()
        rows = []
        for index, (value, status) in enumerate(values):
            rows.append((
                f"history-run-{index}",
                f"adf-history-{index}",
                datetime(2026, 11, 1, 12, 0, 0) + timedelta(days=index),
                metric_name,
                dimension_key,
                "orders",
                float(value),
                None,
                None,
                0,
                None,
                None,
                status,
            ))
        return self.spark.createDataFrame(rows, schema=schema)

    def test_builds_daily_driver_restaurant_and_payment_method_metrics(self):
        observations = build_gold_quality_observations(
            self.gold_outputs,
            run_id="gold-run-1",
            pipeline_run_id="adf-run-1",
            recorded_at_utc="2026-11-10T12:00:00+00:00",
        )
        rows = observations.collect()
        names = {row["metric_name"] for row in rows}

        self.assertTrue({
            "daily_order_count",
            "daily_successful_payment_amount",
            "daily_average_delivery_minutes",
            "daily_average_customer_rating",
            "daily_delivery_completion_rate",
            "daily_cancellation_rate",
            "daily_paid_order_rate",
            "payment_method_failure_rate",
            "driver_delivery_volume",
            "driver_delivery_completion_rate",
            "driver_average_delivery_minutes",
            "restaurant_average_rating",
        }.issubset(names))
        self.assertTrue(all(row["run_id"] == "gold-run-1" for row in rows))
        self.assertTrue(all(row["pipeline_run_id"] == "adf-run-1" for row in rows))
        self.assertTrue(all(row["recorded_at_utc"] is not None for row in rows))

        card_failure_rate = next(
            row["metric_value"] for row in rows
            if row["metric_name"] == "payment_method_failure_rate"
            and row["dimension_key"] == "2026-11-01::CARD"
        )
        self.assertAlmostEqual(0.2, card_failure_rate)

        daily_delivery_rate = next(
            row["metric_value"] for row in rows
            if row["metric_name"] == "daily_delivery_completion_rate"
            and row["dimension_key"] == "2026-11-01"
        )
        self.assertAlmostEqual(0.6, daily_delivery_rate)

    def test_first_observation_is_recorded_without_false_anomaly(self):
        current = self._observations(20)
        history = self.spark.createDataFrame([], quality_metrics_schema())

        result = evaluate_quality_anomalies(current, history).first()

        self.assertEqual("INSUFFICIENT_HISTORY", result["status"])
        self.assertEqual(0, result["baseline_observation_count"])
        self.assertIsNone(result["baseline_mean"])

    def test_large_change_against_stable_history_is_flagged(self):
        current = self._observations(16)
        history = self._history([(10, "NORMAL")] * 5)

        result = evaluate_quality_anomalies(current, history).first()

        self.assertEqual("ANOMALY", result["status"])
        self.assertEqual(5, result["baseline_observation_count"])
        self.assertAlmostEqual(10.0, result["baseline_mean"])
        self.assertAlmostEqual(0.6, result["deviation_ratio"])

    def test_small_change_against_stable_history_is_normal(self):
        current = self._observations(12)
        history = self._history([(10, "NORMAL")] * 5)

        result = evaluate_quality_anomalies(current, history).first()

        self.assertEqual("NORMAL", result["status"])
        self.assertAlmostEqual(0.2, result["deviation_ratio"])

    def test_history_warmup_does_not_flag_large_change(self):
        current = self._observations(100)
        history = self._history([(10, "NORMAL")] * 4)

        result = evaluate_quality_anomalies(current, history).first()

        self.assertEqual("INSUFFICIENT_HISTORY", result["status"])
        self.assertEqual(4, result["baseline_observation_count"])

    def test_prior_anomalies_are_excluded_from_future_baseline(self):
        current = self._observations(12)
        history = self._history(
            [(10, "NORMAL")] * 5 + [(1000, "ANOMALY")]
        )

        result = evaluate_quality_anomalies(current, history).first()

        self.assertEqual("NORMAL", result["status"])
        self.assertEqual(5, result["baseline_observation_count"])
        self.assertAlmostEqual(10.0, result["baseline_mean"])

    def test_baselines_are_isolated_by_dimension(self):
        current = self._observations(100, dimension_key="D-101")
        history = self._history([(10, "NORMAL")] * 5, dimension_key="D-102")

        result = evaluate_quality_anomalies(current, history).first()

        self.assertEqual("INSUFFICIENT_HISTORY", result["status"])
        self.assertEqual(0, result["baseline_observation_count"])

    def test_rejects_invalid_anomaly_thresholds(self):
        current = self._observations(20)
        history = self.spark.createDataFrame([], quality_metrics_schema())

        with self.assertRaisesRegex(ValueError, "at least 1"):
            evaluate_quality_anomalies(current, history, min_history_observations=0)
        with self.assertRaisesRegex(ValueError, "greater than zero"):
            evaluate_quality_anomalies(current, history, z_score_threshold=0)
        with self.assertRaisesRegex(ValueError, "must not be negative"):
            evaluate_quality_anomalies(current, history, absolute_floor=-1)

    def test_rejects_missing_gold_metric_inputs(self):
        with self.assertRaisesRegex(ValueError, "missing quality metric inputs"):
            build_gold_quality_observations(
                {"gold_daily_order_performance": self.gold_outputs["gold_daily_order_performance"]},
                run_id="run",
                pipeline_run_id="adf",
                recorded_at_utc="2026-11-10T12:00:00+00:00",
            )


if __name__ == "__main__":
    unittest.main()
