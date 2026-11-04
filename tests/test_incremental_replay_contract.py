"""Static checks that cumulative API snapshots use replay-safe Silver handling."""
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
API_PIPELINE = json.loads((ROOT / "pipeline" / "api_src_inital load.json").read_text(encoding="utf-8"))

NOTEBOOKS = {
    "payments": {
        "key": 'key_columns=["payment_id"]',
        "version": 'version_column="updated_at"',
        "required": '"updated_at"',
    },
    "deliveries": {
        "key": 'key_columns=["delivery_id"]',
        "version": 'version_column="updated_at"',
        "required": '"updated_at"',
    },
    "ratings": {
        "key": 'key_columns=["rating_id"]',
        "version": 'version_column="updated_at"',
        "required": '"updated_at"',
    },
    "order_status": {
        "key": 'key_columns=["order_id", "status_timestamp"]',
        "version": "version_column=None",
        "required": '"status_timestamp"',
    },
}


class IncrementalReplayContractTests(unittest.TestCase):
    def test_api_pipeline_serializes_runs_and_uses_timestamped_landing_files(self):
        self.assertEqual(1, API_PIPELINE["properties"].get("concurrency"))
        activities = API_PIPELINE["properties"]["activities"]
        foreach = next(item for item in activities if item.get("name") == "foreach_api_endpoint")
        loop = foreach["typeProperties"]["activities"]
        copy = next(item for item in loop if item.get("name") == "Api_migration")
        filename = copy["typeProperties"]["outputs"][0]["parameters"]["p_file_name"]["value"]
        self.assertIn("formatDateTime(utcNow()", filename)
        self.assertIn("endpoint_name", filename)

    def test_each_api_silver_python_export_uses_replay_policy(self):
        for dataset, contract in NOTEBOOKS.items():
            path = ROOT / "notebooks" / f"Silver_{'Order_Status' if dataset == 'order_status' else dataset.title()}.py"
            source = path.read_text(encoding="utf-8")
            with self.subTest(dataset=dataset):
                self.assertIn("from src.common.incremental import deduplicate_replayed_snapshot_rows", source)
                self.assertIn("deduplicate_replayed_snapshot_rows(", source)
                self.assertIn(contract["key"], source)
                self.assertIn(contract["version"], source)
                self.assertIn(contract["required"], source)

    def test_each_api_silver_notebook_export_matches_python_replay_policy(self):
        for dataset, contract in NOTEBOOKS.items():
            stem = "Order_Status" if dataset == "order_status" else dataset.title()
            path = ROOT / "notebooks" / f"Silver_{stem}.ipynb"
            notebook = json.loads(path.read_text(encoding="utf-8"))
            source = "\n".join(
                "".join(cell.get("source", []))
                for cell in notebook.get("cells", [])
                if cell.get("cell_type") == "code"
            )
            with self.subTest(dataset=dataset):
                self.assertIn("from src.common.incremental import deduplicate_replayed_snapshot_rows", source)
                self.assertIn("deduplicate_replayed_snapshot_rows(", source)
                self.assertIn(contract["key"], source)
                self.assertIn(contract["version"], source)
                self.assertIn(contract["required"], source)

    def test_shared_helper_defines_latest_version_and_conflict_policy(self):
        source = (ROOT / "src/common/incremental.py").read_text(encoding="utf-8")
        self.assertIn("Window.partitionBy(*key_columns).orderBy(", source)
        self.assertIn("F.col(\"_replay_version_ts\").desc()", source)
        self.assertIn("conflicting payloads for the same", source)
        self.assertIn("null or unparseable", source)


if __name__ == "__main__":
    unittest.main()
