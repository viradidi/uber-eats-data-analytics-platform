"""Unit tests for Gold versioned publication primitives and SQL contracts."""

import unittest

from pyspark.sql import SparkSession

from src.gold.publication import (
    GOLD_OUTPUT_NAMES,
    active_publication_id,
    build_activation_merge_sql,
    build_publication_view_sql,
    history_table_name,
    publication_history_schema,
    publication_state_schema,
    qualified_name,
    with_publication_id,
)


class GoldPublicationPrimitiveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("uber-eats-gold-publication-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_publication_history_tables_are_defined_for_all_gold_outputs(self):
        self.assertEqual(5, len(GOLD_OUTPUT_NAMES))
        for name in GOLD_OUTPUT_NAMES:
            self.assertEqual(f"{name}__versions", history_table_name(name))
        with self.assertRaisesRegex(ValueError, "Unsupported Gold output"):
            history_table_name("gold_unknown")

    def test_qualified_table_name_validates_each_identifier(self):
        self.assertEqual(
            "`catalog`.`gold`.`gold_order_360`",
            qualified_name("catalog", "gold", "gold_order_360"),
        )
        with self.assertRaisesRegex(ValueError, "Unsafe table identifier"):
            qualified_name("catalog", "gold", "gold; DROP TABLE users")

    def test_stable_view_uses_active_pointer_and_hides_internal_version_key(self):
        sql = build_publication_view_sql(
            "catalog",
            "gold",
            "gold_order_360",
            ["order_id", "current_order_status"],
        )
        self.assertIn("CREATE OR REPLACE VIEW", sql)
        self.assertIn("`gold_order_360__versions`", sql)
        self.assertIn("`gold_publication_state`", sql)
        self.assertIn("v.`order_id`", sql)
        self.assertIn("v.`current_order_status`", sql)
        self.assertIn("v._publication_id = p.active_publication_id", sql)
        self.assertNotIn("v.`_publication_id`", sql)

    def test_view_builder_rejects_unsafe_columns_and_outputs(self):
        with self.assertRaisesRegex(ValueError, "Unsupported Gold output"):
            build_publication_view_sql("c", "s", "gold_unapproved", ["id"])
        with self.assertRaisesRegex(ValueError, "Unsafe Gold output column"):
            build_publication_view_sql("c", "s", "gold_order_360", ["id; DROP"])

    def test_activation_merge_is_compare_and_swap_with_null_safe_match(self):
        sql = build_activation_merge_sql(
            "catalog",
            "gold",
            publication_id="new-run",
            expected_previous_publication_id="old-run",
            pipeline_run_id="adf-run-1",
        )
        self.assertIn("MERGE INTO", sql)
        self.assertIn("target.active_publication_id <=> source.expected_previous_publication_id", sql)
        self.assertIn("'old-run' AS expected_previous_publication_id", sql)
        self.assertIn("'new-run' AS new_publication_id", sql)
        self.assertIn("WHEN NOT MATCHED", sql)

    def test_first_activation_only_inserts_when_no_previous_publication_exists(self):
        sql = build_activation_merge_sql(
            "catalog",
            "gold",
            publication_id="first-run",
            expected_previous_publication_id=None,
            pipeline_run_id="manual",
        )
        self.assertIn("CAST(NULL AS STRING) AS expected_previous_publication_id", sql)
        self.assertIn("WHEN NOT MATCHED\n AND source.expected_previous_publication_id IS NULL", sql)

    def test_activation_merge_escapes_sql_literal_values(self):
        sql = build_activation_merge_sql(
            "catalog",
            "gold",
            publication_id="pub'123",
            expected_previous_publication_id=None,
            pipeline_run_id="adf'789",
        )
        self.assertIn("'pub''123'", sql)
        self.assertIn("'adf''789'", sql)

    def test_publication_id_is_added_without_changing_original_frame(self):
        frame = self.spark.createDataFrame([(1, "PAID")], ["order_id", "status"])
        versioned = with_publication_id(frame, "run-123")
        self.assertEqual(["order_id", "status"], frame.columns)
        self.assertIn("_publication_id", versioned.columns)
        row = versioned.first()
        self.assertEqual("run-123", row["_publication_id"])
        self.assertEqual(1, row["order_id"])

    def test_reserved_publication_column_and_empty_id_are_rejected(self):
        frame = self.spark.createDataFrame([(1, "run")], ["id", "_publication_id"])
        with self.assertRaisesRegex(ValueError, "reserved"):
            with_publication_id(frame, "new-run")
        clean = self.spark.createDataFrame([(1,)], ["id"])
        with self.assertRaisesRegex(ValueError, "non-empty"):
            with_publication_id(clean, "")

    def test_publication_metadata_schemas_include_recovery_fields(self):
        state_fields = set(publication_state_schema().fieldNames())
        history_fields = set(publication_history_schema().fieldNames())
        self.assertTrue({
            "publication_key", "active_publication_id", "updated_at_utc", "pipeline_run_id"
        }.issubset(state_fields))
        self.assertTrue({
            "publication_id", "event_status", "previous_publication_id",
            "output_counts_json", "error_message"
        }.issubset(history_fields))


if __name__ == "__main__":
    unittest.main()
