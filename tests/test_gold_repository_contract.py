"""Static checks for the Gold layer, reporting pack, and ADF orchestration contract."""
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class GoldRepositoryContractTests(unittest.TestCase):
    def test_master_pipeline_waits_for_all_ingestion_paths_before_gold(self):
        path = ROOT / "pipeline" / "uber_eats_master.json"
        pipeline = json.loads(path.read_text(encoding="utf-8"))
        activities = {item["name"]: item for item in pipeline["properties"]["activities"]}
        expected = {"Run_API_Ingestion", "Run_CSV_Ingestion", "Run_SQL_Ingestion"}
        self.assertTrue(expected.issubset(activities))
        for name in expected:
            activity = activities[name]
            self.assertEqual("ExecutePipeline", activity["type"])
            self.assertTrue(activity["typeProperties"]["waitOnCompletion"])
        gold = activities["Publish_Gold_Analytics"]
        self.assertEqual("DatabricksNotebook", gold["type"])
        dependencies = {
            item["activity"]: item["dependencyConditions"]
            for item in gold["dependsOn"]
        }
        self.assertEqual(expected, set(dependencies))
        self.assertTrue(all(value == ["Succeeded"] for value in dependencies.values()))
        self.assertEqual("/Shared/UberEats/notebooks/Gold_Analytics", gold["typeProperties"]["notebookPath"])
        self.assertEqual("ls_api_adb", gold["linkedServiceName"]["referenceName"])
        self.assertEqual("@pipeline().RunId", gold["typeProperties"]["baseParameters"]["pipeline_run_id"]["value"])

    def test_gold_notebook_exports_and_reporting_pack_exist(self):
        self.assertTrue((ROOT / "notebooks" / "Gold_Analytics.py").is_file())
        notebook = json.loads((ROOT / "notebooks" / "Gold_Analytics.ipynb").read_text(encoding="utf-8"))
        self.assertEqual("Gold_Analytics", notebook["metadata"]["application/vnd.databricks.v1+notebook"]["notebookMetadata"]["notebookName"])
        self.assertTrue((ROOT / "reports" / "GOLD_ANALYTICS_REPORTING.sql").is_file())
        self.assertTrue((ROOT / "src" / "gold" / "quality.py").is_file())
        source = (ROOT / "notebooks" / "Gold_Analytics.py").read_text(encoding="utf-8")
        self.assertIn("build_gold_quality_observations", source)
        self.assertIn("evaluate_quality_anomalies", source)
        self.assertIn("gold_quality_metrics", source)
        self.assertTrue((ROOT / "docs" / "GOLD_QUALITY_ANOMALY_MONITORING.md").is_file())
        quality_report = ROOT / "reports" / "GOLD_QUALITY_MONITORING.sql"
        self.assertTrue(quality_report.is_file())
        report_sql = quality_report.read_text(encoding="utf-8")
        self.assertIn("status = 'ANOMALY'", report_sql)
        self.assertIn("ROW_NUMBER() OVER", report_sql)
        self.assertTrue((ROOT / "src" / "gold" / "publication.py").is_file())
        self.assertTrue((ROOT / "docs" / "GOLD_PUBLICATION_AND_RECOVERY.md").is_file())
        publication_report = ROOT / "reports" / "GOLD_PUBLICATION_OPERATIONS.sql"
        self.assertTrue(publication_report.is_file())
        self.assertIn("gold_publication_state", publication_report.read_text(encoding="utf-8"))

    def test_gold_publication_stages_all_outputs_before_atomic_pointer_switch(self):
        source = (ROOT / "notebooks" / "Gold_Analytics.py").read_text(encoding="utf-8")
        publication_module = (ROOT / "src" / "gold" / "publication.py").read_text(encoding="utf-8")

        self.assertIn("GOLD_OUTPUT_NAMES", source)
        self.assertIn("history_table_name(output_name)", source)
        self.assertIn('mode("append")', source)
        self.assertIn("GOLD_PUBLICATION_STAGE_COUNT_MISMATCH", source)
        self.assertIn("build_activation_merge_sql", source)
        self.assertIn("active_publication_id(spark, catalog, gold_schema)", source)
        self.assertIn("ALTER TABLE", source)
        self.assertIn("__legacy_", source)
        self.assertIn("target.active_publication_id <=> source.expected_previous_publication_id", publication_module)
        self.assertIn("CREATE OR REPLACE VIEW", publication_module)
        self.assertNotIn('target_name = f"{catalog}.{gold_schema}.{output_name}"', source)

        staged_event = source.index('"SNAPSHOTS_STAGED"', source.index("output_counts[output_name]"))
        activation = source.index("spark.sql(build_activation_merge_sql", staged_event)
        self.assertLess(staged_event, activation)

        self.assertIn("FAILED_BEFORE_COMMIT", source)
        self.assertIn("SUCCEEDED_WITH_WARNINGS", source)


    def test_gold_retention_is_dry_run_first_and_deployed_with_its_modules(self):
        retention_module = (ROOT / "src" / "gold" / "retention.py").read_text(encoding="utf-8")
        self.assertIn("UNKNOWN_EVENT_STATUS", retention_module)
        self.assertIn("NON_TERMINAL_PUBLICATION", retention_module)
        self.assertIn("NO_PUBLICATION_HISTORY", retention_module)

        maintenance = (ROOT / "notebooks" / "Gold_Publication_Maintenance.py").read_text(encoding="utf-8")
        self.assertIn('dbutils.widgets.text("dry_run", "true"', maintenance)
        self.assertIn("DELETE_OLD_GOLD_PUBLICATIONS", maintenance)
        self.assertIn("MIN_RETENTION_DAYS", maintenance)
        self.assertIn("build_guarded_delete_sql", maintenance)
        self.assertIn("VACUUM", maintenance)
        self.assertTrue((ROOT / "tests" / "test_gold_retention.py").is_file())
        self.assertTrue((ROOT / "docs" / "GOLD_PUBLICATION_RETENTION.md").is_file())

        importer = (ROOT / "deployment" / "databricks" / "import_workspace_assets.ps1").read_text(encoding="utf-8")
        self.assertIn("notebooks/Gold_Publication_Maintenance.py", importer)
        for module in ("quality.py", "publication.py", "retention.py"):
            self.assertIn(f"src/gold/{module}", importer)
        self.assertIn("$WorkspaceRoot/notebooks/Gold_Publication_Maintenance", importer)


    def test_gold_outputs_and_synthetic_scope_are_documented(self):
        guide = (ROOT / "docs" / "GOLD_LAYER_AND_REPORTING.md").read_text(encoding="utf-8")
        self.assertIn("gold_order_360", guide)
        self.assertIn("gold_daily_order_performance", guide)
        self.assertIn("synthetic", guide.lower())
        self.assertIn("does not include a deployed power bi workspace", guide.lower())


if __name__ == "__main__":
    unittest.main()
