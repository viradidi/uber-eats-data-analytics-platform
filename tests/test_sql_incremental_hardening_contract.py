"""Static guards for SQL incremental metadata validation and cursor precision."""
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = json.loads((ROOT / "pipeline/sql_incremental pipeline.json").read_text(encoding="utf-8"))
CONTROL_DATASET = json.loads((ROOT / "dataset/DS_sql_control.json").read_text(encoding="utf-8"))
SOURCE_WATER_DATASET = json.loads((ROOT / "dataset/ds_sql_water.json").read_text(encoding="utf-8"))
MIGRATION = (
    ROOT / "sql/migrations/20261104_widen_pipeline_checkpoint_to_datetime2_7.sql"
).read_text(encoding="utf-8")
REVIEW = (ROOT / "docs/SQL_INCREMENTAL_LOAD_REVIEW.md").read_text(encoding="utf-8")
SOURCE_CONTRACT = (ROOT / "docs/INCREMENTAL_SOURCE_CONTRACT.md").read_text(encoding="utf-8")
PREFLIGHT = (ROOT / "docs/SQL_INCREMENTAL_CONTROL_PREFLIGHT.sql").read_text(encoding="utf-8")
SOURCE_DIAGNOSTIC = (ROOT / "sql/diagnostics/inspect_source_contract_candidates.sql").read_text(encoding="utf-8")
SQL_SILVER_MIGRATION = (ROOT / "sql/migrations/20261105_create_sql_silver_control.sql").read_text(encoding="utf-8")
SQL_SILVER_NOTEBOOK = (ROOT / "notebooks/Silver_SQL_Incremental.py").read_text(encoding="utf-8")
SQL_SILVER_HELPER = (ROOT / "src/common/sql_silver.py").read_text(encoding="utf-8")
DATABRICKS_IMPORTER = (ROOT / "deployment/databricks/import_workspace_assets.ps1").read_text(encoding="utf-8")


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


    def test_control_table_bootstrap_creates_only_verified_api_control_rows(self):
        bootstrap = (
            ROOT / "sql/migrations/20261104_create_ingestion_control_tables.sql"
        ).read_text(encoding="utf-8").lower()
        self.assertIn("create table dbo.pipeline_control", bootstrap)
        self.assertIn("create table dbo.api_pipeline_control", bootstrap)
        self.assertIn("last_watermark_value datetime2(7) null", bootstrap)
        self.assertIn("check (is_active = 0 or last_watermark_value is not null)", bootstrap)
        self.assertIn("ux_pipeline_control_active_source_cursor", bootstrap)
        self.assertIn("ux_api_pipeline_control_endpoint_name", bootstrap)
        for endpoint in ("payments", "deliveries", "ratings", "order-status"):
            with self.subTest(endpoint=endpoint):
                self.assertIn(f"('{endpoint}', 'api/", bootstrap)
        self.assertIn("where not exists", bootstrap)
        self.assertIn("no sql source control rows or watermark values were created", bootstrap)
        self.assertIn("begin catch", bootstrap)
        self.assertIn("rollback transaction", bootstrap)

    def test_only_active_control_rows_are_processed(self):
        lookup = find_activity(self.top, "Lookup_SQL_Control")
        query = lookup["typeProperties"]["source"]["sqlReaderQuery"]
        self.assertIn("FROM dbo.pipeline_control AS pc", query)
        self.assertIn("LEFT JOIN dbo.sql_silver_control AS cfg", query)
        self.assertIn("WHERE pc.is_active = 1", query)
        active_field = next(
            column for column in CONTROL_DATASET["properties"]["schema"]
            if column["name"] == "is_active"
        )
        self.assertEqual("bit", active_field["type"])
        self.assertIn("WHERE is_active = 1", PREFLIGHT)
        self.assertIn("inspect_source_contract_candidates.sql", PREFLIGHT)

    def test_duplicate_active_control_metadata_fails_before_loop(self):
        lookup = find_activity(self.top, "Validate active SQL control metadata")
        gate = find_activity(self.top, "Active SQL control metadata is unique")
        query = lookup["typeProperties"]["source"]["sqlReaderQuery"]
        self.assertIn("GROUP BY control_id", query)
        self.assertIn("control_id IS NULL", query)
        self.assertIn("GROUP BY schema_name, table_name, watermark_column", query)
        control_id_check = query.split("OR EXISTS")[0]
        source_mapping_check = query.split("OR EXISTS")[1]
        self.assertNotIn("WHERE is_active", control_id_check)
        self.assertIn("WHERE is_active = 1", source_mapping_check)
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
            ["Active SQL control metadata is unique"],
            [item["activity"] for item in gate["dependsOn"]],
        )
        self.assertEqual(
            ["Require approved SQL Silver metadata"],
            [item["activity"] for item in loop["dependsOn"]],
        )
        silver_gate = find_activity(self.top, "Require approved SQL Silver metadata")
        self.assertEqual(
            ["Require active SQL control rows"],
            [item["activity"] for item in silver_gate["dependsOn"]],
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

    def test_split_source_and_control_databases_use_the_correct_linked_services(self):
        source_linked_service = SOURCE_WATER_DATASET["properties"]["linkedServiceName"]["referenceName"]
        control_linked_service = CONTROL_DATASET["properties"]["linkedServiceName"]["referenceName"]
        self.assertEqual("ls_sql_database", source_linked_service)
        self.assertEqual("ls_sql_control", control_linked_service)
        self.assertEqual("ds_sql_water", self.validation["typeProperties"]["dataset"]["referenceName"])

        null_check = find_activity(self.activities, "Validate SQL watermark nulls")
        self.assertEqual("ds_sql", null_check["typeProperties"]["dataset"]["referenceName"])
        self.assertEqual("ds_sql", self.check["typeProperties"]["dataset"]["referenceName"])
        self.assertEqual("ds_sql_water", self.maximum["typeProperties"]["dataset"]["referenceName"])

        branch = find_activity(self.activities, "rowscount is there")["typeProperties"]["ifTrueActivities"]
        checkpoint = find_activity(branch, "update watermark_copy1")
        self.assertEqual("ls_sql_control", checkpoint["linkedServiceName"]["referenceName"])

        restore_script = (ROOT / "deployment/azure/configure_restore_environment.ps1").read_text(encoding="utf-8")
        self.assertIn('Set-SqlLinkedService "linkedService/ls_sql_database.json" $SourceSqlServerFqdn $SourceSqlDatabase', restore_script)
        self.assertIn('Set-SqlLinkedService "linkedService/ls_sql_control.json" $ControlSqlServerFqdn $ControlSqlDatabase', restore_script)

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

    def test_sql_landing_filename_suffix_matches_delimited_sink_extension(self):
        copy = self.copy
        sink = copy["typeProperties"]["sink"]
        self.assertEqual(".csv", sink["formatSettings"]["fileExtension"])
        output = copy["outputs"][0]
        self.assertEqual("ds_adls", output["referenceName"])
        filename = output["parameters"]["p_file_name"]["value"]
        self.assertIn("'.csv')", filename)
        self.assertIn("p_file_name", output["parameters"])

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

    def test_source_contract_inventory_is_read_only_and_row_scans_are_opt_in(self):
        for token in (
            "DECLARE @CollectRowStats bit = 0",
            "sys.schemas",
            "sys.tables",
            "sys.columns",
            "sys.types",
            "sys.indexes",
            "QUOTENAME(",
            "THROW 51200",
            "THROW 51201",
            "filter_definition",
            "NO_PRIMARY_OR_UNIQUE_INDEX_CANDIDATE",
        ):
            self.assertIn(token, SOURCE_DIAGNOSTIC)
        upper = SOURCE_DIAGNOSTIC.upper()
        for forbidden in (
            "UPDATE DBO.",
            "DELETE FROM DBO.",
            "MERGE DBO.",
            "ALTER TABLE DBO.",
            "DROP TABLE DBO.",
            "TRUNCATE TABLE DBO.",
        ):
            self.assertNotIn(forbidden, upper)
        self.assertIn("source owners must approve keys and cursor semantics", SOURCE_DIAGNOSTIC)

    def test_sql_silver_is_a_repository_owned_notebook_and_checkpoint_waits_for_it(self):
        branch = find_activity(self.activities, "rowscount is there")["typeProperties"]["ifTrueActivities"]
        silver = find_activity(branch, "Job_Silver_copy1")
        self.assertEqual("DatabricksNotebook", silver["type"])
        self.assertEqual(
            "/Shared/UberEats/notebooks/Silver_SQL_Incremental",
            silver["typeProperties"]["notebookPath"],
        )
        params = silver["typeProperties"]["baseParameters"]
        for name in (
            "control_id", "schema_name", "table_name", "target_table",
            "watermark_column", "lower_watermark", "upper_watermark",
            "business_key_columns_json", "required_columns_json",
            "column_types_json", "bronze_root", "batch_file_name",
        ):
            self.assertIn(name, params)
        self.assertIn("ctl',string(item().control_id)", params["batch_file_name"]["value"])
        self.assertEqual(
            "Succeeded",
            find_activity(branch, "update watermark_copy1")["dependsOn"][0]["dependencyConditions"][0],
        )
        self.assertEqual("Job_Silver_copy1", find_activity(branch, "update watermark_copy1")["dependsOn"][0]["activity"])

    def test_active_sql_sources_require_enabled_silver_contract_before_copy(self):
        lookup = find_activity(self.top, "Lookup_SQL_Control")
        self.assertIn("LEFT JOIN dbo.sql_silver_control", lookup["typeProperties"]["source"]["sqlReaderQuery"])
        validation = find_activity(self.top, "Validate active SQL Silver metadata")
        self.assertIn("ISJSON", validation["typeProperties"]["source"]["sqlReaderQuery"])
        gate = find_activity(self.top, "Require approved SQL Silver metadata")
        self.assertEqual(
            ["Require approved SQL Silver metadata"],
            [item["activity"] for item in find_activity(self.top, "ForEach item")["dependsOn"]],
        )
        self.assertEqual(
            ["Succeeded"],
            [item["dependencyConditions"][0] for item in gate["dependsOn"]],
        )
        self.assertIn("SQL_SILVER_METADATA_NOT_READY", gate["typeProperties"]["ifFalseActivities"][0]["typeProperties"]["errorCode"])
        dataset_columns = {
            item["name"] for item in CONTROL_DATASET["properties"]["schema"]
        }
        self.assertTrue({
            "sql_silver_is_enabled", "sql_silver_write_strategy",
            "business_key_columns_json", "required_columns_json", "column_types_json",
        }.issubset(dataset_columns))

    def test_sql_silver_metadata_migration_is_fail_closed_and_seeds_no_source_contracts(self):
        for token in (
            "CREATE TABLE dbo.sql_silver_control",
            "FOREIGN KEY (control_id) REFERENCES dbo.pipeline_control(control_id)",
            "CHECK (write_strategy = 'DELTA_MERGE')",
            "CHECK (",
            "ISJSON(business_key_columns_json)",
            "ISJSON(required_columns_json)",
            "ISJSON(column_types_json)",
            "is_enabled = 0",
            "No source configurations were created or enabled",
        ):
            self.assertIn(token, SQL_SILVER_MIGRATION)
        self.assertEqual([], list(ROOT.glob("sql/sources/*")))

    def test_sql_silver_notebook_reads_one_exact_file_and_merges_with_contract_helper(self):
        self.assertIn('.load(f"{BRONZE_ROOT}/{table_name}/{batch_file_name}")', SQL_SILVER_NOTEBOOK)
        self.assertIn('batch_file_name.endswith(".csv")', SQL_SILVER_NOTEBOOK)
        self.assertNotIn('option("recursiveFileLookup", "true")', SQL_SILVER_NOTEBOOK)
        for token in (
            "parse_identifier_list",
            "parse_column_types",
            "prepare_sql_silver_batch",
            "publish_delta_merge",
            "write_sql_silver_audit",
        ):
            self.assertIn(token, SQL_SILVER_NOTEBOOK)
        self.assertIn("DeltaTable.forName", SQL_SILVER_HELPER)
        self.assertIn("whenMatchedUpdate", SQL_SILVER_HELPER)
        self.assertIn("whenNotMatchedInsert", SQL_SILVER_HELPER)
        self.assertIn("same watermark version", SQL_SILVER_HELPER)
        self.assertIn("duplicate business-key groups", SQL_SILVER_HELPER)
        self.assertIn('Silver_SQL_Incremental.py', DATABRICKS_IMPORTER)
        self.assertIn('"src/common/sql_silver.py"', DATABRICKS_IMPORTER)
        self.assertNotIn("Configure the SQL Silver Job separately", DATABRICKS_IMPORTER)

    def test_equal_or_older_late_arrivals_and_deletes_are_not_claimed_as_solved(self):
        self.assertIn("Late-arriving rows and equal watermark values", REVIEW)
        self.assertIn("strict lower bound", REVIEW)
        self.assertIn("hard deletes", REVIEW)
        self.assertIn("a watermark is a progress cursor", SOURCE_CONTRACT.lower())


if __name__ == "__main__":
    unittest.main()
