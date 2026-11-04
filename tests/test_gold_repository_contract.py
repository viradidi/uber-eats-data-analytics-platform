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
        self.assertEqual("/Shared/UberEats/Gold_Analytics", gold["typeProperties"]["notebookPath"])
        self.assertEqual("ls_api_adb", gold["linkedServiceName"]["referenceName"])
        self.assertEqual("@pipeline().RunId", gold["typeProperties"]["baseParameters"]["pipeline_run_id"]["value"])

    def test_gold_notebook_exports_and_reporting_pack_exist(self):
        self.assertTrue((ROOT / "notebooks" / "Gold_Analytics.py").is_file())
        notebook = json.loads((ROOT / "notebooks" / "Gold_Analytics.ipynb").read_text(encoding="utf-8"))
        self.assertEqual("Gold_Analytics", notebook["metadata"]["application/vnd.databricks.v1+notebook"]["notebookMetadata"]["notebookName"])
        self.assertTrue((ROOT / "reports" / "GOLD_ANALYTICS_REPORTING.sql").is_file())

    def test_gold_outputs_and_synthetic_scope_are_documented(self):
        guide = (ROOT / "docs" / "GOLD_LAYER_AND_REPORTING.md").read_text(encoding="utf-8")
        self.assertIn("gold_order_360", guide)
        self.assertIn("gold_daily_order_performance", guide)
        self.assertIn("synthetic", guide.lower())
        self.assertIn("does not include a deployed power bi workspace", guide.lower())


if __name__ == "__main__":
    unittest.main()
