"""Static contract tests for the repeatable ADF control-table bootstrap."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT / "sql/migrations/20261104_create_ingestion_control_tables.sql"
).read_text(encoding="utf-8").lower()


class ControlTableBootstrapContractTests(unittest.TestCase):
    def test_creates_both_control_tables_and_required_adf_fields(self):
        self.assertIn("create table dbo.pipeline_control", MIGRATION)
        self.assertIn("create table dbo.api_pipeline_control", MIGRATION)
        for column in (
            "control_id",
            "source_name",
            "schema_name",
            "table_name",
            "load_type",
            "watermark_column",
            "last_watermark_value",
            "batch_size",
            "target_table",
            "is_active",
        ):
            with self.subTest(table="pipeline_control", column=column):
                self.assertIn(column, MIGRATION)
        for column in (
            "control_id",
            "endpoint_name",
            "target_folder",
            "is_active",
            "last_watermark_value",
        ):
            with self.subTest(table="api_pipeline_control", column=column):
                self.assertIn(column, MIGRATION)

    def test_sql_checkpoint_uses_datetime2_seven_and_requires_seeded_value_when_active(self):
        self.assertIn("last_watermark_value datetime2(7) null", MIGRATION)
        self.assertIn("check (is_active = 0 or last_watermark_value is not null)", MIGRATION)
        self.assertIn("no sql source control rows or watermark values were created", MIGRATION)

    def test_api_control_seeds_only_the_four_documented_endpoints(self):
        expected = (
            "('payments', 'api/payments')",
            "('deliveries', 'api/deliveries')",
            "('ratings', 'api/ratings')",
            "('order-status', 'api/order_status')",
        )
        for endpoint in expected:
            with self.subTest(endpoint=endpoint):
                self.assertIn(endpoint, MIGRATION)

    def test_existing_api_endpoint_state_is_not_reactivated_or_reset(self):
        self.assertIn("where not exists", MIGRATION)
        self.assertIn("set target.target_folder = source.target_folder", MIGRATION)
        self.assertNotIn("set target.is_active", MIGRATION)
        self.assertNotIn("set target.last_watermark_value", MIGRATION)

    def test_rerun_safety_and_rollback_are_explicit(self):
        self.assertGreaterEqual(MIGRATION.count("if object_id"), 2)
        self.assertIn("if not exists", MIGRATION)
        self.assertIn("begin transaction", MIGRATION)
        self.assertIn("begin catch", MIGRATION)
        self.assertIn("rollback transaction", MIGRATION)
        self.assertIn("commit transaction", MIGRATION)


if __name__ == "__main__":
    unittest.main()
