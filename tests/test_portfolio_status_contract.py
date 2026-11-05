"""Keep portfolio validation claims aligned with the current deployment and CI evidence."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CI_RUN_URL = "https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/41984330196"
STATUS_FILES = (
    "README.md",
    "docs/PROJECT_CHECKLIST.md",
    "docs/FINAL_HANDOFF.md",
    "docs/PORTFOLIO_EVIDENCE.md",
    "docs/PROJECT_RELEASE_GATE.md",
    "docs/RUN_EVIDENCE_RETENTION.md",
    "architecture/architecture.md",
    "docs/RELEASE_READINESS_REVIEW.md",
)


class PortfolioStatusContractTests(unittest.TestCase):
    def test_status_documents_point_to_the_verified_132_test_ci_run(self):
        for relative_path in STATUS_FILES:
            with self.subTest(path=relative_path):
                content = (ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn(CI_RUN_URL, content)
                self.assertIn("132 tests", content)

    def test_runtime_acceptance_is_distinguished_from_repository_ci(self):
        acceptance = (ROOT / "docs/DEPLOYMENT_ACCEPTANCE_RECORD.md").read_text(encoding="utf-8")
        self.assertIn("deployed and end-to-end operation verified", acceptance.lower())
        self.assertIn("project owner", acceptance.lower())
        self.assertIn("not supplied", acceptance.lower())
        for relative_path in STATUS_FILES:
            with self.subTest(path=relative_path):
                content = (ROOT / relative_path).read_text(encoding="utf-8").lower()
                self.assertTrue("azure" in content or "databricks" in content)
                self.assertNotIn("restored azure acceptance pending", content)
                self.assertNotIn("awaiting a fresh azure", content)

    def test_only_evidence_curation_remains_open_in_the_project_checklist(self):
        checklist = (ROOT / "docs/PROJECT_CHECKLIST.md").read_text(encoding="utf-8")
        open_items = [
            line.strip()
            for line in checklist.splitlines()
            if line.lstrip().startswith("- [ ]")
        ]
        self.assertEqual(1, len(open_items), open_items)
        self.assertIn("actual ADF and Databricks run IDs", open_items[0])
        self.assertIn("evidence curation only", open_items[0].lower())

    def test_deleted_external_job_is_replaced_by_repository_owned_sql_silver(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        handoff = (ROOT / "docs/FINAL_HANDOFF.md").read_text(encoding="utf-8")
        release_gate = (ROOT / "docs/PROJECT_RELEASE_GATE.md").read_text(encoding="utf-8")
        pipeline = (ROOT / "pipeline/sql_incremental pipeline.json").read_text(encoding="utf-8")
        importer = (ROOT / "deployment/databricks/import_workspace_assets.ps1").read_text(encoding="utf-8")
        self.assertIn("Silver_SQL_Incremental", readme)
        self.assertIn("deleted", handoff.lower())
        self.assertIn("sql_silver_control", release_gate.lower())
        self.assertIn('"type": "DatabricksNotebook"', pipeline)
        self.assertIn("/Shared/UberEats/notebooks/Silver_SQL_Incremental", pipeline)
        self.assertIn('Silver_SQL_Incremental.py', importer)
        self.assertIn("SQL_SILVER_JOB_RECONSTRUCTION.md", readme)


if __name__ == "__main__":
    unittest.main()
