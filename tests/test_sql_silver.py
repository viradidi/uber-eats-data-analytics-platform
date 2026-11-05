"""Unit tests for metadata parsing and SQL Silver batch validation."""

import unittest
from datetime import datetime

from pyspark.sql import SparkSession

from src.common.sql_silver import (
    parse_column_types,
    parse_identifier_list,
    prepare_sql_silver_batch,
    validate_target_table,
)


class SqlSilverContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("sql-silver-contract-tests")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_metadata_json_is_validated_and_normalized(self):
        self.assertEqual(
            ["order_id", "order_line_id"],
            parse_identifier_list('["order_id", "order_line_id"]', "keys"),
        )
        self.assertEqual(
            {"order_id": "string", "modified_at": "timestamp"},
            parse_column_types(
                '{"order_id":"STRING","modified_at":"TIMESTAMP"}'
            ),
        )

    def test_metadata_rejects_duplicate_or_unsafe_names_and_unknown_types(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            parse_identifier_list('["id", "id"]', "keys")
        with self.assertRaisesRegex(ValueError, "unsafe"):
            parse_identifier_list('["id; DROP TABLE x"]', "keys")
        with self.assertRaisesRegex(ValueError, "Unsupported Spark type"):
            parse_column_types('{"id":"map<string,string>"}')

    def test_target_table_must_be_fully_qualified_and_safe(self):
        self.assertEqual(
            "ubereats.silver.orders",
            validate_target_table("ubereats.silver.orders"),
        )
        with self.assertRaisesRegex(ValueError, "fully qualified"):
            validate_target_table("silver.orders; DROP TABLE x")

    def test_batch_is_typed_bounded_and_rejects_duplicate_keys(self):
        raw = self.spark.createDataFrame(
            [
                ("O-1", "2026-01-01 10:00:01", "12.50"),
                ("O-2", "2026-01-01 10:00:02", "14.00"),
            ],
            ["order_id", "modified_at", "amount"],
        )
        batch, metrics = prepare_sql_silver_batch(
            raw,
            business_key_columns='["order_id"]',
            required_columns='["order_id","modified_at"]',
            column_types='{"order_id":"string","modified_at":"timestamp","amount":"decimal(10,2)"}',
            watermark_column="modified_at",
            lower_watermark="2026-01-01T10:00:00",
            upper_watermark="2026-01-01T10:01:00",
        )
        self.assertEqual(2, metrics["source_rows"])
        self.assertEqual("decimal(10,2)", batch.schema["amount"].dataType.simpleString())

        duplicate = self.spark.createDataFrame(
            [
                ("O-1", "2026-01-01 10:00:01", "12.50"),
                ("O-1", "2026-01-01 10:00:02", "13.50"),
            ],
            ["order_id", "modified_at", "amount"],
        )
        with self.assertRaisesRegex(ValueError, "duplicate business-key"):
            prepare_sql_silver_batch(
                duplicate,
                business_key_columns='["order_id"]',
                required_columns='["order_id"]',
                column_types='{"order_id":"string","modified_at":"timestamp","amount":"decimal(10,2)"}',
                watermark_column="modified_at",
                lower_watermark=datetime(2026, 1, 1, 10, 0, 0),
                upper_watermark=datetime(2026, 1, 1, 10, 1, 0),
            )

    def test_batch_rejects_bad_casts_and_out_of_range_rows(self):
        raw = self.spark.createDataFrame(
            [("O-1", "not-a-date", "abc")],
            ["order_id", "modified_at", "amount"],
        )
        with self.assertRaisesRegex(ValueError, "fail the approved column type"):
            prepare_sql_silver_batch(
                raw,
                business_key_columns='["order_id"]',
                required_columns='[]',
                column_types='{"order_id":"string","modified_at":"timestamp","amount":"decimal(10,2)"}',
                watermark_column="modified_at",
                lower_watermark="2026-01-01T10:00:00",
                upper_watermark="2026-01-01T10:01:00",
            )

        out_of_range = self.spark.createDataFrame(
            [("O-1", "2026-01-01 10:02:00", "12.00")],
            ["order_id", "modified_at", "amount"],
        )
        with self.assertRaisesRegex(ValueError, "outside its declared"):
            prepare_sql_silver_batch(
                out_of_range,
                business_key_columns='["order_id"]',
                required_columns='[]',
                column_types='{"order_id":"string","modified_at":"timestamp","amount":"decimal(10,2)"}',
                watermark_column="modified_at",
                lower_watermark="2026-01-01T10:00:00",
                upper_watermark="2026-01-01T10:01:00",
            )


if __name__ == "__main__":
    unittest.main()
