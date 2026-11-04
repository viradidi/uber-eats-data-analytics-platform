"""Keep portfolio validation claims aligned with the latest repository evidence."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CI_RUN_URL = "https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/41843392449"
STATUS_FILES = (
    "README.md",
    "docs/PROJECT_CHECKLIST.md",
    "docs/FINAL_HANDOFF.md",
    "docs/PORTFOLIO_EVIDENCE.md",
    "docs/PROJECT_RELEASE_GATE.md",
    "docs/RUN_EVIDENCE_RETENTION.md",
)


class PortfolioStatusContractTests(unittest.TestCase):
    def test_status_documents_point_to_the_verified_90_test_ci_run(self):
        for relative_path in STATUS_FILES:
            with self.subTest(path=relative_path):
                content = (ROOT / relative_path).read_text(encoding="utf-8")
                self.assertIn(CI_RUN_URL, content)
                self.assertIn("90 tests", content)

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

    def test_external_sql_silver_job_is_not_claimed_as_repository_verified(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        handoff = (ROOT / "docs/FINAL_HANDOFF.md").read_text(encoding="utf-8")
        release_gate = (ROOT / "docs/PROJECT_RELEASE_GATE.md").read_text(encoding="utf-8")
        for content in (readme, handoff, release_gate):
            self.assertIn("121055824417886", content)
            self.assertIn("not included in this repository", content.lower())
        self.assertIn("same bounded interval twice", release_gate)


if __name__ == "__main__":
    unittest.main()
