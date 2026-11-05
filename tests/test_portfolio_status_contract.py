"""Keep portfolio validation claims aligned with the latest repository evidence."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CI_RUN_URL = "https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/41901084241"
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
    def test_status_documents_point_to_the_verified_121_test_ci_run(self):
        for relative_path in STATUS_FILES:
            with self.subTest(path=relative_path):
                content = (ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn(CI_RUN_URL, content)
                self.assertIn("121 tests", content)

    def test_local_spark_ci_is_not_misrepresented_as_live_cloud_validation(self):
        for relative_path in STATUS_FILES:
            with self.subTest(path=relative_path):
                content = (ROOT / relative_path).read_text(encoding="utf-8").lower()
                self.assertTrue(
                    "azure" in content or "databricks" in content,
                    f"{relative_path} should preserve the cloud-validation boundary",
                )
        checklist = (ROOT / "docs/PROJECT_CHECKLIST.md").read_text(encoding="utf-8")
        self.assertIn("[x] Run the local PySpark tests in GitHub Actions", checklist)
        self.assertIn("[ ] Import Gold_Analytics into the target Databricks workspace", checklist)
        self.assertIn("[ ] Complete and retain the source-specific incremental contract", checklist)

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
