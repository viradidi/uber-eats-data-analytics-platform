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
PREFLIGHT = (ROOT / "docs/SQL_INCREMENTAL_CONTROL_PREFLIGHT.sql").read_text(encoding="utf-8")


def find_activity(activities, name):
    for activity in activities:
        if activity.get("name") == name:
            return activity
    raise AssertionError(f"Activity not found: {name}")


class SqlIncrementalHardeningContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.top = PIPELINE["properties"]["activities"]
        foreach = find_activity(cls.top, "ForEach item")
        cls.activities = foreach["typeProperties"]["activities"]
        cls.validation = find_activity(cls.activities, "Validate SQL source metadata")
        cls.gate = find_activity(cls.activities, "SQL source metadata is valid")
        cls.check = find_activity(cls.activities, "Check any new data")
        condition = find_activity(cls.activities, "rowscount is there")
        branch = condition["typeProperties"]["ifTrueActivities"]
        cls.copy = find_activity(branch, "adls data migration_copy1")
        cls.maximum = find_activity(branch, "Get max watermark_copy1")


    def test_only_active_control_rows_are_processed(self):
        lookup = find_activity(self.top, "Lookup_SQL_Control")
        query = lookup["typeProperties"]["source"]["sqlReaderQuery"]
        self.assertIn("FROM [dbo].[pipeline_control]", query)
        self.assertIn("WHERE [is_active] = 1", query)
        active_field = next(
            column for column in CONTROL_DATASET["properties"]["schema"]
            if column["name"] == "is_active"
        )
        self.assertEqual("bit", active_field["type"])
        self.assertIn("WHERE is_active = 1", PREFLIGHT)
        self.assertIn("WHERE pc.is_active = 1", PREFLIGHT)

    def test_duplicate_active_control_metadata_fails_before_loop(self):
        lookup = find_activity(self.top, "Validate active SQL control metadata")
        gate = find_activity(self.top, "Active SQL control metadata is unique")
        query = lookup["typeProperties"]["source"]["sqlReaderQuery"]
        self.assertIn("GROUP BY control_id", query)
        self.assertIn("control_id IS NULL", query)
        self.assertIn("GROUP BY schema_name, table_name, watermark_column", query)
        self.assertIn("WHERE is_active = 1", query)
        self.assertEqual(
            ["Validate active SQL control metadata"],
            [item["activity"] for item in gate["dependsOn"]],
        )
        fail = gate["typeProperties"]["ifFalseActivities"][0]
        self.assertEqual("Fail", fail["type"])
        self.assertEqual(
            "SQL_DUPLICATE_ACTIVE_CONTROL_METADATA",
            fail["typeProperties"]["errorCode"],
        )
        active_gate = find_activity(self.top, "Require active SQL control rows")
        self.assertEqual(
            ["Active SQL control metadata is unique"],
            [item["activity"] for item in active_gate["dependsOn"]],
        )

    def test_pipeline_fails_when_no_sql_control_rows_are_active(self):
        gate = find_activity(self.top, "Require active SQL control rows")
        loop = find_activity(self.top, "ForEach item")
        expression = gate["typeProperties"]["expression"]["value"]
        self.assertIn("length(activity('Lookup_SQL_Control').output.value)", expression)
        self.assertNotIn("Count active SQL control rows", [item["name"] for item in self.top])
        self.assertEqual(
            ["Lookup_SQL_Control"],
            [item["activity"] for item in gate["dependsOn"]],
        )
        self.assertEqual(
            ["Require active SQL control rows"],
            [item["activity"] for item in loop["dependsOn"]],
        )
        fail = gate["typeProperties"]["ifFalseActivities"][0]
        self.assertEqual("Fail", fail["type"])
        self.assertEqual("SQL_NO_ACTIVE_CONTROL_ROWS", fail["typeProperties"]["errorCode"])

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
