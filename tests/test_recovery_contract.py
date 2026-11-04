"""Static safeguards for the exported ADF incremental pipeline and its contracts."""
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PIPELINE = json.loads((ROOT / "pipeline" / "sql_incremental pipeline.json").read_text(encoding="utf-8"))
RUNBOOK = (ROOT / "docs/RECOVERY_AND_RERUN_RUNBOOK.md").read_text(encoding="utf-8")
REVIEW = (ROOT / "docs/SQL_INCREMENTAL_LOAD_REVIEW.md").read_text(encoding="utf-8")
CONCURRENCY = (ROOT / "docs/CHECKPOINT_CONCURRENCY_CONTRACT.md").read_text(encoding="utf-8")
SOURCE = (ROOT / "docs/INCREMENTAL_SOURCE_CONTRACT.md").read_text(encoding="utf-8")
OPERATIONS = (ROOT / "docs/OPERATIONS_AND_RELIABILITY.md").read_text(encoding="utf-8")


def find_activity(activities, name):
    for activity in activities:
        if activity.get("name") == name:
            return activity
    raise AssertionError(f"Activity not found: {name}")


def deps(activity):
    return {item["activity"]: item.get("dependencyConditions", []) for item in activity.get("dependsOn", [])}


class IncrementalPipelineRecoveryContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.top = PIPELINE["properties"]["activities"]
        foreach = find_activity(cls.top, "ForEach item")
        cls.loop = foreach["typeProperties"]["activities"]
        condition = find_activity(cls.loop, "rowscount is there")
        cls.branch = condition["typeProperties"]["ifTrueActivities"]
        cls.copy = find_activity(cls.branch, "adls data migration_copy1")
        cls.maximum = find_activity(cls.branch, "Get max watermark_copy1")
        cls.silver = find_activity(cls.branch, "Job_Silver_copy1")
        cls.update = find_activity(cls.branch, "update watermark_copy1")

    def test_pipeline_serializes_runs_and_uses_checkpoint_compare_and_swap(self):
        self.assertEqual(1, PIPELINE["properties"].get("concurrency"))
        script = self.update["typeProperties"]["scripts"][0]["text"]["value"]
        self.assertIn("last_watermark_value", script)
        self.assertIn("@@ROWCOUNT <> 1", script)
        self.assertIn("THROW 51001", script)
        self.assertIn("does not protect the control table from other pipelines", CONCURRENCY)

    def test_copy_is_bounded_by_captured_upper_watermark(self):
        copy_deps = deps(self.copy)
        self.assertIn("Get max watermark_copy1", copy_deps)
        self.assertIn("Succeeded", copy_deps["Get max watermark_copy1"])
        query = self.copy["typeProperties"]["source"]["sqlReaderQuery"]["value"]
        self.assertIn("item().last_watermark_value", query)
        self.assertIn("activity('Get max watermark_copy1').output.firstRow.max_watermark", query)
        self.assertIn(" <= ", query)

    def test_checkpoint_waits_for_silver_success(self):
        self.assertIn("Succeeded", deps(self.silver)["adls data migration_copy1"])
        self.assertIn("Succeeded", deps(self.update)["Job_Silver_copy1"])
        self.assertIn("Advance a watermark/checkpoint only when all applicable conditions are true", RUNBOOK)
        self.assertIn("Checkpoint advanced only after required work succeeded", RUNBOOK)

    def test_source_contract_documents_incremental_edge_cases(self):
        for term in ("Initial-load strategy", "Stable business key", "late-visible row", "Delete capture", "Replay and deduplication rule"):
            self.assertIn(term.lower(), SOURCE.lower())
        self.assertIn("approved allowlist", SOURCE.lower())
        self.assertIn("restrict write access to the control table", SOURCE.lower())

    def test_sql_review_documents_limits_and_metadata_risks(self):
        for term in (
            "Null or uninitialized watermark",
            "Late-arriving rows and equal watermark values",
            "deployment-specific allowlist",
            "untrusted input",
            "idempotent",
            "source-metadata preflight",
            "datetime2(7)",
        ):
            self.assertIn(term.lower(), REVIEW.lower())
        self.assertIn("counts source rows whose watermark is greater", REVIEW.lower())
        self.assertIn("does not, by itself, guarantee", REVIEW.lower())

    def test_runbook_and_operations_document_safe_recovery(self):
        for term in ("do not edit it merely to make a run progress", "partial", "checkpoint", "does not claim automated rollback"):
            self.assertIn(term.lower(), RUNBOOK.lower())
        self.assertIn("azure resources were later decommissioned", OPERATIONS.lower())
        for report in ("OPERATIONAL_HEALTH_REPORT.sql", "DATA_QUALITY_TRENDS_AND_INCIDENTS.sql", "RECONCILIATION_AUDIT_QUERIES.sql"):
            self.assertTrue((ROOT / "docs" / report).is_file())
            self.assertIn(report.lower(), OPERATIONS.lower())


if __name__ == "__main__":
    unittest.main()
