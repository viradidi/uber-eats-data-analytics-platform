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




def _validate_activity_scopes(activities, errors, scope="root"):
    """Validate unique sibling names and dependency references in exported ADF scopes."""
    names = [activity.get("name") for activity in activities]
    named = [name for name in names if name]
    if len(named) != len(set(named)):
        errors.append(f"{scope}: duplicate activity names")
    known = set(named)
    for activity in activities:
        name = activity.get("name", "<unnamed>")
        for dependency in activity.get("dependsOn", []):
            dependency_name = dependency.get("activity")
            if dependency_name not in known:
                errors.append(f"{scope}/{name}: dependency {dependency_name!r} is not a sibling activity")
            if not dependency.get("dependencyConditions"):
                errors.append(f"{scope}/{name}: dependency {dependency_name!r} has no conditions")
        props = activity.get("typeProperties", {})
        if activity.get("type") == "ForEach":
            children = props.get("activities", [])
            _validate_activity_scopes(children, errors, f"{scope}/{name}")
        elif activity.get("type") == "IfCondition":
            for branch in ("ifTrueActivities", "ifFalseActivities"):
                children = props.get(branch, [])
                if children:
                    _validate_activity_scopes(children, errors, f"{scope}/{name}/{branch}")

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

    def test_all_adf_activity_dependencies_reference_unique_siblings(self):
        errors = []
        _validate_activity_scopes(self.top_activities, errors)
        self.assertEqual([], errors, "Invalid ADF activity graph:\\n" + "\\n".join(errors))

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

    def test_source_contract_covers_incremental_edge_cases(self):
        contract_path = ROOT / "docs" / "INCREMENTAL_SOURCE_CONTRACT.md"
        self.assertTrue(contract_path.is_file())
        contract = contract_path.read_text(encoding="utf-8")
        for requirement in (
            "Initial-load behavior",
            "Stable business key",
            "Late-arriving rows",
            "Delete capture",
            "Replay behavior",
            "Invalid table/column metadata",
            "checkpoint must remain unchanged",
        ):
            self.assertIn(requirement, contract)

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

    def test_change_presence_lookup_is_not_misrepresented_as_a_bounded_extract(self):
        check = _find_activity(self.foreach_activities, "Check any new data")
        query = check["typeProperties"]["source"]["sqlReaderQuery"]["value"]
        self.assertIn("item().last_watermark_value", query)
        self.assertIn("item().watermark_column", query)
        review_path = ROOT / "docs" / "SQL_INCREMENTAL_LOAD_REVIEW.md"
        review = review_path.read_text(encoding="utf-8")
        self.assertIn("change-presence check only", review)
        self.assertIn("not a completeness guarantee", review)

    def test_dynamic_sql_identifiers_are_flagged_as_a_deployment_gate(self):
        check = _find_activity(self.foreach_activities, "Check any new data")
        check_query = check["typeProperties"]["source"]["sqlReaderQuery"]["value"]
        copy_query = self.copy["typeProperties"]["source"]["sqlReaderQuery"]["value"]
        max_query = self.max_watermark["typeProperties"]["source"]["sqlReaderQuery"]["value"]

        # These are interpolated SQL identifiers, not parameterized values.
        self.assertIn("item().table_name", check_query)
        self.assertIn("item().watermark_column", check_query)
        self.assertIn("item().table_name", copy_query)
        self.assertIn("item().watermark_column", copy_query)
        self.assertIn("item().table_name", max_query)
        self.assertIn("item().watermark_column", max_query)

        review_path = ROOT / "docs" / "SQL_INCREMENTAL_LOAD_REVIEW.md"
        review = review_path.read_text(encoding="utf-8")
        self.assertIn("does not show an allowlist guard", review)
        self.assertIn("Do not populate these fields from untrusted input", review)

    def test_timestamped_landing_files_are_not_misrepresented_as_idempotent(self):
        output_parameters = self.copy["outputs"][0]["parameters"]
        file_name = output_parameters["p_file_name"]["value"]
        self.assertIn("formatDateTime(utcNow()", file_name)

        review_path = ROOT / "docs" / "SQL_INCREMENTAL_LOAD_REVIEW.md"
        review = review_path.read_text(encoding="utf-8")
        self.assertIn("A retry can therefore create a second file", review)
        self.assertIn("does not guarantee idempotent Silver processing", review)

    def test_sql_control_preflight_checks_metadata_and_duplicate_mappings(self):
        preflight_path = ROOT / "docs" / "SQL_INCREMENTAL_CONTROL_PREFLIGHT.sql"
        self.assertTrue(preflight_path.is_file())
        preflight = preflight_path.read_text(encoding="utf-8")
        for expected in (
            "FROM dbo.pipeline_control",
            "last_watermark_value IS NULL",
            "table_name LIKE '%[^A-Za-z0-9_]%'",
            "watermark_column LIKE '%[^A-Za-z0-9_]%'",
            "GROUP BY control_id",
            "GROUP BY table_name, watermark_column",
            "NOT an authorization boundary",
            "FROM sys.tables AS t",
            "ON c.object_id = t.object_id",
            "watermark_data_type",
            "object exists; verify watermark type and source semantics",
        ):
            self.assertIn(expected, preflight)

    def test_control_table_metadata_is_treated_as_privileged_configuration(self):
        contract_path = ROOT / "docs" / "INCREMENTAL_SOURCE_CONTRACT.md"
        contract = contract_path.read_text(encoding="utf-8")
        self.assertIn("approved allowlist", contract)
        self.assertIn("Restrict write access to the control table", contract)
        self.assertIn("Invalid table/column metadata is rejected", contract)


if __name__ == "__main__":
    unittest.main()
