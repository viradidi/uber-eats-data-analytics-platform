"""Static guards for SQL incremental metadata validation and cursor precision."""
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = json.loads((ROOT / "pipeline/sql_incremental pipeline.json").read_text(encoding="utf-8"))
CONTROL_DATASET = json.loads((ROOT / "dataset/DS_sql_control.json").read_text(encoding="utf-8"))
MIGRATION = (
    ROOT / "sql/migrations/20261104_widen_pipeline_checkpoint_to_datetime2_7.sql"
).read_text(encoding="utf-8")
REVIEW = (ROOT / "docs/SQL_INCREMENTAL_LOAD_REVIEW.md").read_text(encoding="utf-8")
SOURCE_CONTRACT = (ROOT / "docs/INCREMENTAL_SOURCE_CONTRACT.md").read_text(encoding="utf-8")


def find_activity(activities, name):
    for activity in activities:
        if activity.get("name") == name:
            return activity
    raise AssertionError(f"Activity not found: {name}")


class SqlIncrementalHardeningContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        foreach = find_activity(PIPELINE["properties"]["activities"], "ForEach item")
        cls.activities = foreach["typeProperties"]["activities"]
        cls.validation = find_activity(cls.activities, "Validate SQL source metadata")
        cls.gate = find_activity(cls.activities, "SQL source metadata is valid")
        cls.check = find_activity(cls.activities, "Check any new data")
        condition = find_activity(cls.activities, "rowscount is there")
        branch = condition["typeProperties"]["ifTrueActivities"]
        cls.copy = find_activity(branch, "adls data migration_copy1")
        cls.maximum = find_activity(branch, "Get max watermark_copy1")

    def test_source_metadata_validation_precedes_incremental_count(self):
        names = [activity["name"] for activity in self.activities]
        self.assertLess(names.index("Validate SQL source metadata"), names.index("SQL source metadata is valid"))
        self.assertLess(names.index("SQL source metadata is valid"), names.index("Check any new data"))
        self.assertEqual(
            ["SQL watermark values are valid"],
            [item["activity"] for item in self.check.get("dependsOn", [])],
        )
        fail_activities = self.gate["typeProperties"]["ifFalseActivities"]
        self.assertEqual("Fail", fail_activities[0]["type"])
        self.assertEqual("SQL_SOURCE_METADATA_INVALID", fail_activities[0]["typeProperties"]["errorCode"])

    def test_metadata_validation_checks_schema_table_column_and_safe_identifier_contract(self):
        query = self.validation["typeProperties"]["source"]["sqlReaderQuery"]["value"]
        for token in (
            "sys.schemas",
            "sys.tables",
            "sys.columns",
            "item().schema_name",
            "item().table_name",
            "item().watermark_column",
            "datetime2",
            "NOT LIKE",
            "replace(",
        ):
            self.assertIn(token, query)
        self.assertIn("SQL source metadata is invalid", self.gate["typeProperties"]["ifFalseActivities"][0]["typeProperties"]["message"])

    def test_null_watermark_rows_fail_before_incremental_count(self):
        null_check = find_activity(self.activities, "Validate SQL watermark nulls")
        null_gate = find_activity(self.activities, "SQL watermark values are valid")
        query = null_check["typeProperties"]["source"]["sqlReaderQuery"]["value"]
        self.assertIn("IS NULL", query)
        self.assertIn("item().schema_name", query)
        self.assertIn("item().table_name", query)
        self.assertIn("item().watermark_column", query)
        self.assertEqual(
            ["SQL watermark values are valid"],
            [item["activity"] for item in self.check.get("dependsOn", [])],
        )
        self.assertEqual(
            "SQL_SOURCE_NULL_WATERMARK",
            null_gate["typeProperties"]["ifFalseActivities"][0]["typeProperties"]["errorCode"],
        )

    def test_source_extraction_honors_schema_and_bracket_quotes_validated_identifiers(self):
        count_query = self.check["typeProperties"]["source"]["sqlReaderQuery"]["value"]
        copy_query = self.copy["typeProperties"]["source"]["sqlReaderQuery"]["value"]
        max_query = self.maximum["typeProperties"]["source"]["sqlReaderQuery"]["value"]
        for query in (count_query, copy_query, max_query):
            self.assertIn("item().schema_name", query)
            self.assertIn("item().table_name", query)
            self.assertIn("item().watermark_column", query)
            self.assertIn("[', item().schema_name, '].[', item().table_name, ']", query)

    def test_bounded_window_and_checkpoint_success_dependency_are_preserved(self):
        copy_query = self.copy["typeProperties"]["source"]["sqlReaderQuery"]["value"]
        branch = find_activity(self.activities, "rowscount is there")["typeProperties"]["ifTrueActivities"]
        update = find_activity(branch, "update watermark_copy1")
        silver = find_activity(branch, "Job_Silver_copy1")
        self.assertIn(" > ", copy_query)
        self.assertIn(" <= ", copy_query)
        update_deps = {d["activity"]: d["dependencyConditions"] for d in update["dependsOn"]}
        self.assertEqual(["Succeeded"], update_deps["Job_Silver_copy1"])
        silver_deps = {d["activity"]: d["dependencyConditions"] for d in silver["dependsOn"]}
        self.assertEqual(["Succeeded"], silver_deps["adls data migration_copy1"])

    def test_control_checkpoint_dataset_declares_seven_digit_datetime2(self):
        field = next(
            column for column in CONTROL_DATASET["properties"]["schema"]
            if column["name"] == "last_watermark_value"
        )
        self.assertEqual("datetime2", field["type"])
        self.assertEqual(7, field["scale"])
        self.assertEqual(27, field["precision"])

    def test_precision_migration_preserves_nullability_and_fails_closed(self):
        for token in (
            "ALTER COLUMN last_watermark_value datetime2(7)",
            "CASE WHEN @is_nullable = 1 THEN N' NULL' ELSE N' NOT NULL'",
            "THROW 51020",
            "THROW 51021",
            "THROW 51022",
            "THROW 51023",
            "BEGIN TRANSACTION",
            "ROLLBACK TRANSACTION",
        ):
            self.assertIn(token, MIGRATION)

    def test_equal_or_older_late_arrivals_and_deletes_are_not_claimed_as_solved(self):
        self.assertIn("Late-arriving rows and equal watermark values", REVIEW)
        self.assertIn("strict lower bound", REVIEW)
        self.assertIn("hard deletes", REVIEW)
        self.assertIn("a watermark is a progress cursor", SOURCE_CONTRACT.lower())


if __name__ == "__main__":
    unittest.main()
