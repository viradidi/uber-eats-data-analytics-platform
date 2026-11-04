"""Local Spark tests for replay-safe cumulative API snapshot handling."""
import unittest

from pyspark.sql import SparkSession

from src.common.incremental import deduplicate_replayed_snapshot_rows


class IncrementalReplayTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("uber-eats-incremental-replay-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_exact_full_snapshot_replays_collapse_by_business_key(self):
        schema = ["payment_id", "order_id", "amount", "updated_at"]
        frame = self.spark.createDataFrame([
            ("P1", "O1", 10.0, "2026-09-01 10:00:00"),
            ("P2", "O2", 20.0, "2026-09-01 10:05:00"),
            ("P1", "O1", 10.0, "2026-09-01 10:00:00"),
            ("P2", "O2", 20.0, "2026-09-01 10:05:00"),
        ], schema)

        result = deduplicate_replayed_snapshot_rows(
            frame, ["payment_id"], "updated_at", "payments"
        )

        self.assertEqual(2, result.count())
        self.assertEqual(2, result.select("payment_id").distinct().count())

    def test_latest_source_version_wins_even_when_older_snapshot_arrives_later(self):
        frame = self.spark.createDataFrame([
            ("P1", "O1", 15.0, "2026-09-01 13:00:00"),
            ("P1", "O1", 10.0, "2026-09-01 10:00:00"),
            ("P2", "O2", 20.0, "2026-09-01 11:00:00"),
        ], ["payment_id", "order_id", "amount", "updated_at"])

        result = deduplicate_replayed_snapshot_rows(
            frame, ["payment_id"], "updated_at", "payments"
        )
        records = {row["payment_id"]: row.asDict() for row in result.collect()}

        self.assertEqual(15.0, records["P1"]["amount"])
        self.assertEqual("2026-09-01 13:00:00", records["P1"]["updated_at"])
        self.assertEqual(20.0, records["P2"]["amount"])

    def test_conflicting_payloads_at_same_key_and_version_fail_closed(self):
        frame = self.spark.createDataFrame([
            ("P1", "O1", 10.0, "2026-09-01 10:00:00"),
            ("P1", "O1", 99.0, "2026-09-01 10:00:00"),
        ], ["payment_id", "order_id", "amount", "updated_at"])

        with self.assertRaisesRegex(ValueError, "conflicting payloads"):
            deduplicate_replayed_snapshot_rows(
                frame, ["payment_id"], "updated_at", "payments"
            )

    def test_event_snapshot_replays_collapse_without_losing_distinct_events(self):
        frame = self.spark.createDataFrame([
            ("O1", "PLACED", "2026-09-01 10:00:00"),
            ("O1", "DELIVERED", "2026-09-01 10:45:00"),
            ("O1", "PLACED", "2026-09-01 10:00:00"),
        ], ["order_id", "status", "status_timestamp"])

        result = deduplicate_replayed_snapshot_rows(
            frame, ["order_id", "status_timestamp"], None, "order_status"
        )

        self.assertEqual(2, result.count())
        statuses = {row["status"] for row in result.collect()}
        self.assertEqual({"PLACED", "DELIVERED"}, statuses)

    def test_conflicting_event_at_same_composite_key_fails_without_version(self):
        frame = self.spark.createDataFrame([
            ("O1", "PLACED", "2026-09-01 10:00:00"),
            ("O1", "PREPARING", "2026-09-01 10:00:00"),
        ], ["order_id", "status", "status_timestamp"])

        with self.assertRaisesRegex(ValueError, "no source version column"):
            deduplicate_replayed_snapshot_rows(
                frame, ["order_id", "status_timestamp"], None, "order_status"
            )

    def test_null_keys_and_invalid_versions_are_rejected(self):
        null_key = self.spark.createDataFrame(
            [(None, "O1", 10.0, "2026-09-01 10:00:00")],
            "payment_id STRING, order_id STRING, amount DOUBLE, updated_at STRING",
        )
        with self.assertRaisesRegex(ValueError, "null business keys"):
            deduplicate_replayed_snapshot_rows(
                null_key, ["payment_id"], "updated_at", "payments"
            )

        invalid_version = self.spark.createDataFrame(
            [("P1", "O1", 10.0, "not-a-timestamp")],
            ["payment_id", "order_id", "amount", "updated_at"],
        )
        with self.assertRaisesRegex(ValueError, "unparseable updated_at"):
            deduplicate_replayed_snapshot_rows(
                invalid_version, ["payment_id"], "updated_at", "payments"
            )

    def test_empty_incremental_batch_remains_empty(self):
        frame = self.spark.createDataFrame(
            [],
            "payment_id STRING, order_id STRING, amount DOUBLE, updated_at STRING",
        )
        result = deduplicate_replayed_snapshot_rows(
            frame, ["payment_id"], "updated_at", "payments"
        )
        self.assertEqual(0, result.count())


if __name__ == "__main__":
    unittest.main()
