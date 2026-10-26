"""Structural and business-rule checks for committed CSV source fixtures.

These tests validate repository sample data, not live Azure ingestion or Spark jobs.
Run with: python -m unittest discover -s tests -v
"""
import csv
import unittest
from datetime import date
from pathlib import Path

from src.common.quality_events import (
    _build_snapshot_checks,
    _snapshot_values_match,
    reconcile_full_snapshot,
)

ROOT = Path(__file__).resolve().parents[1]


def read_csv(name):
    with (ROOT / name).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return reader.fieldnames or [], rows


def assert_required_columns(testcase, actual, required, filename):
    testcase.assertTrue(actual, f"{filename}: header row is missing")
    testcase.assertTrue(
        set(required).issubset(actual),
        f"{filename}: missing columns {sorted(set(required) - set(actual))}",
    )


def assert_nonblank(testcase, rows, column, filename):
    for line_number, row in enumerate(rows, start=2):
        testcase.assertTrue(
            (row.get(column) or "").strip(),
            f"{filename}:{line_number}: {column} must not be blank",
        )


def assert_unique(testcase, rows, columns, filename):
    seen = set()
    for line_number, row in enumerate(rows, start=2):
        key = tuple((row.get(column) or "").strip() for column in columns)
        testcase.assertTrue(
            all(key),
            f"{filename}:{line_number}: key {columns} contains a blank value",
        )
        testcase.assertNotIn(
            key, seen, f"{filename}:{line_number}: duplicate key {key}"
        )
        seen.add(key)


class SourceFixtureQualityTests(unittest.TestCase):
    def test_drivers_schema_keys_and_domains(self):
        filename = "drivers.csv"
        fields, rows = read_csv(filename)
        assert_required_columns(
            self, fields,
            ["driver_id", "driver_name", "phone", "vehicle_type", "city", "join_date", "status"],
            filename,
        )
        self.assertGreater(len(rows), 0, f"{filename}: expected sample rows")
        assert_nonblank(self, rows, "driver_id", filename)
        assert_nonblank(self, rows, "join_date", filename)
        assert_unique(self, rows, ["driver_id"], filename)
        for line_number, row in enumerate(rows, start=2):
            self.assertIn(row["vehicle_type"].strip().upper(), {"SCOOTER", "BIKE"},
                          f"{filename}:{line_number}: unsupported vehicle_type")
            self.assertIn(row["status"].strip().upper(), {"ACTIVE", "INACTIVE"},
                          f"{filename}:{line_number}: unsupported status")
            try:
                date.fromisoformat(row["join_date"].strip())
            except ValueError:
                self.fail(f"{filename}:{line_number}: join_date must be ISO YYYY-MM-DD")

    def test_promotions_schema_keys_and_rules(self):
        filename = "promotions.csv"
        fields, rows = read_csv(filename)
        assert_required_columns(
            self, fields,
            ["promo_id", "restaurant_id", "promo_code", "discount_type",
             "discount_value", "start_date", "end_date", "status"],
            filename,
        )
        self.assertGreater(len(rows), 0, f"{filename}: expected sample rows")
        assert_nonblank(self, rows, "promo_id", filename)
        assert_unique(self, rows, ["promo_id", "restaurant_id"], filename)
        for line_number, row in enumerate(rows, start=2):
            self.assertIn(row["discount_type"].strip().upper(), {"PERCENTAGE", "FIXED"},
                          f"{filename}:{line_number}: unsupported discount_type")
            try:
                discount = float(row["discount_value"])
            except (TypeError, ValueError):
                self.fail(f"{filename}:{line_number}: discount_value must be numeric")
            self.assertGreater(discount, 0, f"{filename}:{line_number}: discount must be positive")
            try:
                start = date.fromisoformat(row["start_date"].strip())
                end = date.fromisoformat(row["end_date"].strip())
            except ValueError:
                self.fail(f"{filename}:{line_number}: dates must use ISO YYYY-MM-DD")
            self.assertLessEqual(start, end, f"{filename}:{line_number}: start_date exceeds end_date")

    def test_zones_schema_keys_and_coordinates(self):
        filename = "zones.csv"
        fields, rows = read_csv(filename)
        assert_required_columns(
            self, fields, ["zone_id", "city", "zone_name", "latitude", "longitude"], filename
        )
        self.assertGreater(len(rows), 0, f"{filename}: expected sample rows")
        assert_nonblank(self, rows, "zone_id", filename)
        assert_nonblank(self, rows, "city", filename)
        assert_nonblank(self, rows, "zone_name", filename)
        assert_unique(self, rows, ["zone_id"], filename)
        for line_number, row in enumerate(rows, start=2):
            try:
                latitude = float(row["latitude"])
                longitude = float(row["longitude"])
            except (TypeError, ValueError):
                self.fail(f"{filename}:{line_number}: coordinates must be numeric")
            self.assertGreaterEqual(latitude, -90, f"{filename}:{line_number}: invalid latitude")
            self.assertLessEqual(latitude, 90, f"{filename}:{line_number}: invalid latitude")
            self.assertGreaterEqual(longitude, -180, f"{filename}:{line_number}: invalid longitude")
            self.assertLessEqual(longitude, 180, f"{filename}:{line_number}: invalid longitude")


class SnapshotReconciliationGuardTests(unittest.TestCase):
    def test_incremental_source_is_rejected_before_spark_access(self):
        with self.assertRaisesRegex(ValueError, "source_is_full_snapshot=True"):
            reconcile_full_snapshot(
                spark=None,
                source_df=None,
                target_table="silver.example",
                dataset_name="example",
                key_columns=["id"],
                source_is_full_snapshot=False,
            )

    def test_empty_business_keys_are_rejected_before_spark_access(self):
        with self.assertRaisesRegex(ValueError, "at least one business key"):
            reconcile_full_snapshot(
                spark=None,
                source_df=None,
                target_table="silver.example",
                dataset_name="example",
                key_columns=[],
                source_is_full_snapshot=True,
            )


    def test_target_only_keys_fail_full_snapshot_coverage_check(self):
        checks = _build_snapshot_checks(
            source_only_keys=0,
            target_only_keys=2,
            source_values={"__row_count": 3},
            target_values={"__row_count": 5},
            aggregate_specs={},
        )
        result = {name: passed for _, name, _, _, passed in checks}
        self.assertFalse(result["target_only_key_count"])
        self.assertFalse(result["row_count"])
        self.assertTrue(result["source_only_key_count"])

    def test_aggregate_mismatch_is_detected(self):
        checks = _build_snapshot_checks(
            source_only_keys=0,
            target_only_keys=0,
            source_values={"__row_count": 3, "__aggregate_amount": 42.0},
            target_values={"__row_count": 3, "__aggregate_amount": 41.0},
            aggregate_specs={"amount": "sum"},
        )
        result = {name: passed for _, name, _, _, passed in checks}
        self.assertTrue(result["row_count"])
        self.assertFalse(result["sum:amount"])

    def test_numeric_tolerance_accepts_small_float_drift(self):
        self.assertTrue(_snapshot_values_match(10.0, 10.0 + 1e-10))
        self.assertFalse(_snapshot_values_match(10.0, 10.1))

    def test_unsupported_aggregate_is_rejected_before_spark_access(self):
        with self.assertRaisesRegex(ValueError, "Unsupported aggregate specifications"):
            reconcile_full_snapshot(
                spark=None,
                source_df=None,
                target_table="silver.example",
                dataset_name="example",
                key_columns=["id"],
                source_is_full_snapshot=True,
                aggregate_specs={"amount": "median"},
            )


if __name__ == "__main__":
    unittest.main()
