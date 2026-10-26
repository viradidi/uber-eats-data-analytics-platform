"""Static safeguards for the committed SQL incremental ADF export and recovery docs.

These tests validate repository structure and dependency contracts only. They do not
execute ADF expressions or prove runtime behavior in Azure.
"""
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIPELINE_PATH = ROOT / "pipeline" / "sql_incremental pipeline.json"
RUNBOOK_PATH = ROOT / "docs" / "RECOVERY_AND_RERUN_RUNBOOK.md"
OPERATIONS_PATH = ROOT / "docs" / "OPERATIONS_AND_RELIABILITY.md"


def _find_activity(activities, name):
    for activity in activities:
        if activity.get("name") == name:
            return activity
    raise AssertionError(f"Activity not found: {name}")


def _dependency_names(activity):
    return {
        dependency["activity"]: dependency.get("dependencyConditions", [])
        for dependency in activity.get("dependsOn", [])
    }


class IncrementalPipelineRecoveryContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.pipeline = json.loads(PIPELINE_PATH.read_text(encoding="utf-8"))
        cls.top_activities = cls.pipeline["properties"]["activities"]
        foreach = _find_activity(cls.top_activities, "ForEach item")
        cls.foreach_activities = foreach["typeProperties"]["activities"]
        condition = _find_activity(cls.foreach_activities, "rowscount is there")
        cls.true_activities = condition["typeProperties"]["ifTrueActivities"]
        cls.copy = _find_activity(cls.true_activities, "adls data migration_copy1")
        cls.max_watermark = _find_activity(cls.true_activities, "Get max watermark_copy1")
        cls.silver_job = _find_activity(cls.true_activities, "Job_Silver_copy1")
        cls.watermark_update = _find_activity(cls.true_activities, "update watermark_copy1")
        cls.runbook = RUNBOOK_PATH.read_text(encoding="utf-8")
        cls.operations = OPERATIONS_PATH.read_text(encoding="utf-8")

    def test_copy_waits_for_bounded_max_watermark_lookup(self):
        dependencies = _dependency_names(self.copy)
        self.assertIn("Get max watermark_copy1", dependencies)
        self.assertIn("Succeeded", dependencies["Get max watermark_copy1"])
        query = self.copy["typeProperties"]["source"]["sqlReaderQuery"]["value"]
        self.assertIn("item().last_watermark_value", query)
        self.assertIn("activity('Get max watermark_copy1').output.firstRow.max_watermark", query)
        self.assertIn(" <= ", query)

    def test_review_documents_null_watermark_and_late_update_limits(self):
        review_path = ROOT / "docs" / "SQL_INCREMENTAL_LOAD_REVIEW.md"
        review = review_path.read_text(encoding="utf-8")
        self.assertIn("Null or uninitialized watermark", review)
        self.assertIn("late-arriving updates", review)
        self.assertIn("same watermark value", review)
        self.assertIn("source-specific decision", review)
        self.assertIn("not yet handled by the exported expressions", review)

    def test_silver_job_waits_for_copy_success(self):
        dependencies = _dependency_names(self.silver_job)
        self.assertIn("adls data migration_copy1", dependencies)
        self.assertIn("Succeeded", dependencies["adls data migration_copy1"])

    def test_watermark_update_waits_for_silver_job_success(self):
        dependencies = _dependency_names(self.watermark_update)
        self.assertIn("Job_Silver_copy1", dependencies)
        self.assertIn("Succeeded", dependencies["Job_Silver_copy1"])
        script = self.watermark_update["typeProperties"]["scripts"][0]["text"]["value"]
        self.assertIn("UPDATE dbo.pipeline_control SET last_watermark_value", script)
        self.assertIn("activity('Get max watermark_copy1').output.firstRow.max_watermark", script)
        self.assertIn("item().control_id", script)

    def test_runbook_requires_success_before_checkpoint_advancement(self):
        self.assertIn("Advance a watermark/checkpoint only when all applicable conditions are true", self.runbook)
        self.assertIn("Checkpoint advanced only after required work succeeded", self.runbook)
        self.assertIn("Do not advance a watermark", self.runbook)

    def test_runbook_disclaims_unverified_live_recovery(self):
        self.assertIn("have not been live-tested in Azure", self.runbook)
        self.assertIn("does not claim automated rollback", self.runbook)
        self.assertIn("live Azure recovery", self.operations)

    def test_operational_reports_are_linked_from_runbook_or_operations_guide(self):
        self.assertTrue((ROOT / "docs" / "OPERATIONAL_HEALTH_REPORT.sql").is_file())
        self.assertTrue((ROOT / "docs" / "DATA_QUALITY_TRENDS_AND_INCIDENTS.sql").is_file())
        self.assertTrue((ROOT / "docs" / "RECONCILIATION_AUDIT_QUERIES.sql").is_file())
        self.assertIn("Operational health report", self.operations)
        self.assertIn("Data-quality trends and incident analysis", self.operations)
        self.assertIn("Reconciliation audit queries", self.operations)


if __name__ == "__main__":
    unittest.main()
