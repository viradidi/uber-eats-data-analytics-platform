"""Behavioral tests for the mock API's timestamp and pagination contract.

These tests exercise Python API helpers with in-memory records. They do not require
Azure or an HTTP server and do not claim to validate deployed infrastructure.
"""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

import main as api


class ApiTimestampContractTests(unittest.TestCase):
    def test_naive_timestamp_is_interpreted_as_utc(self):
        naive = api.parse_timestamp("2026-10-01T12:30:00")
        utc = api.parse_timestamp("2026-10-01T12:30:00Z")
        self.assertEqual(utc, naive)
        self.assertEqual(0, naive.utcoffset().total_seconds())

    def test_equivalent_offsets_normalize_to_same_instant(self):
        left = api.parse_timestamp("2026-10-01T12:00:00Z")
        right = api.parse_timestamp("2026-10-01T14:00:00+02:00")
        self.assertEqual(left, right)

    def test_invalid_timestamp_returns_none(self):
        self.assertIsNone(api.parse_timestamp("not-a-timestamp"))
        self.assertIsNone(api.parse_timestamp(None))

    def test_incremental_filter_is_strictly_greater_than_checkpoint(self):
        records = [
            {"id": 1, "updated_at": "2026-10-01T12:00:00Z"},
            {"id": 2, "updated_at": "2026-10-01T12:00:01Z"},
            {"id": 3, "updated_at": "2026-10-01T12:01:00+00:00"},
        ]
        with patch.object(api, "load_json_file", return_value=records):
            result = api.process_data(
                "unused.json", "updated_at", page=1, page_size=10,
                updated_after="2026-10-01T12:00:00Z",
            )
        self.assertEqual([2, 3], [row["id"] for row in result["data"]])
        self.assertEqual(2, result["pagination"]["total_records"])

    def test_equal_timestamp_records_are_excluded_by_current_cursor_contract(self):
        # This is an intentional characterization of the API's current strict
        # greater-than cursor. Sources with tied timestamps need a composite
        # cursor, overlap window, or CDC contract to avoid missing tied rows.
        records = [
            {"id": 1, "updated_at": "2026-10-01T12:00:00Z"},
            {"id": 2, "updated_at": "2026-10-01T12:00:00+00:00"},
            {"id": 3, "updated_at": "2026-10-01T12:00:01Z"},
        ]
        with patch.object(api, "load_json_file", return_value=records):
            result = api.process_data(
                "unused.json", "updated_at", page=1, page_size=10,
                updated_after="2026-10-01T12:00:00Z",
            )
        self.assertEqual([3], [row["id"] for row in result["data"]])

    def test_watermark_returns_none_when_no_valid_timestamps_exist(self):
        records = [
            {"updated_at": "not-a-timestamp"},
            {"updated_at": None},
            {},
        ]
        with patch.object(api, "load_json_file", return_value=records):
            watermark = api.get_max_timestamp("unused.json", "updated_at")
        self.assertIsNone(watermark)

    def test_watermark_is_none_for_an_empty_source(self):
        with patch.object(api, "load_json_file", return_value=[]):
            watermark = api.get_max_timestamp("unused.json", "updated_at")
        self.assertIsNone(watermark)

    def test_watermark_uses_latest_parseable_timestamp_normalized_to_utc(self):
        records = [
            {"updated_at": "2026-10-01T12:00:00Z"},
            {"updated_at": "2026-10-01T15:30:00+02:00"},
            {"updated_at": "invalid"},
            {},
        ]
        with patch.object(api, "load_json_file", return_value=records):
            watermark = api.get_max_timestamp("unused.json", "updated_at")
        self.assertEqual("2026-10-01T13:30:00Z", watermark)

    def test_incremental_filter_skips_missing_or_unparseable_source_timestamps(self):
        records = [
            {"id": 1, "updated_at": "2026-10-01T12:01:00Z"},
            {"id": 2},
            {"id": 3, "updated_at": "broken"},
        ]
        with patch.object(api, "load_json_file", return_value=records):
            result = api.process_data(
                "unused.json", "updated_at", page=1, page_size=10,
                updated_after="2026-10-01T12:00:00Z",
            )
        self.assertEqual([1], [row["id"] for row in result["data"]])

    def test_invalid_checkpoint_is_rejected(self):
        with patch.object(api, "load_json_file", return_value=[]):
            with self.assertRaises(HTTPException) as raised:
                api.process_data(
                    "unused.json", "updated_at", page=1, page_size=10,
                    updated_after="invalid",
                )
        self.assertEqual(400, raised.exception.status_code)

    def test_pagination_reports_total_before_page_slice(self):
        records = [{"id": n} for n in range(7)]
        with patch.object(api, "load_json_file", return_value=records):
            result = api.process_data(
                "unused.json", "updated_at", page=2, page_size=3,
            )
        self.assertEqual([3, 4, 5], [row["id"] for row in result["data"]])
        self.assertEqual(7, result["pagination"]["total_records"])
        self.assertEqual(3, result["pagination"]["total_pages"])

    def test_every_data_endpoint_declares_api_key_dependency(self):
        from fastapi.routing import APIRoute

        protected_paths = {
            "/payments", "/deliveries", "/ratings", "/order-status",
            "/watermarks", "/payments/generate-test-data",
        }
        routes = {
            route.path: route
            for route in api.app.routes
            if isinstance(route, APIRoute)
        }
        self.assertTrue(protected_paths.issubset(routes))
        for path in protected_paths:
            with self.subTest(path=path):
                dependency_calls = {
                    dependency.call
                    for dependency in routes[path].dependant.dependencies
                }
                self.assertIn(api.verify_api_key, dependency_calls)

    def test_root_discovery_endpoint_is_public_by_design(self):
        from fastapi.routing import APIRoute

        root = next(
            route for route in api.app.routes
            if isinstance(route, APIRoute) and route.path == "/"
        )
        dependency_calls = {
            dependency.call for dependency in root.dependant.dependencies
        }
        self.assertNotIn(api.verify_api_key, dependency_calls)

    def test_missing_api_key_configuration_fails_closed(self):
        with patch.object(api, "API_KEY", None):
            with self.assertRaises(HTTPException) as raised:
                api.verify_api_key("provided-key")
        self.assertEqual(503, raised.exception.status_code)

    def test_wrong_api_key_is_rejected(self):
        with patch.object(api, "API_KEY", "expected-key"):
            with self.assertRaises(HTTPException) as raised:
                api.verify_api_key("wrong-key")
        self.assertEqual(401, raised.exception.status_code)

    def test_payment_id_uses_highest_existing_numeric_suffix(self):
        records = [
            {"payment_id": "P1001"},
            {"payment_id": "P1008"},
            {"payment_id": "P1003"},
            {"payment_id": "legacy-id"},
            {"payment_id": "Pbad"},
        ]
        self.assertEqual(1009, api._next_payment_number(records))

    def test_atomic_json_write_replaces_file_with_valid_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "payments.json"
            path.write_text('[{"payment_id": "P1001"}]', encoding="utf-8")
            payload = [{"payment_id": "P1002", "amount": 25.5}]

            api._write_json_atomically(str(path), payload)

            self.assertEqual(payload, json.loads(path.read_text(encoding="utf-8")))
            self.assertEqual(["payments.json"], sorted(item.name for item in Path(directory).iterdir()))

    def test_atomic_json_replace_failure_preserves_original_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "payments.json"
            original = '[{"payment_id": "P1001"}]'
            path.write_text(original, encoding="utf-8")

            with patch.object(api.os, "replace", side_effect=OSError("simulated replace failure")):
                with self.assertRaises(OSError):
                    api._write_json_atomically(str(path), [{"payment_id": "P1002"}])

            self.assertEqual(original, path.read_text(encoding="utf-8"))
            self.assertEqual(["payments.json"], sorted(item.name for item in Path(directory).iterdir()))

    def test_data_paths_are_absolute_and_independent_of_working_directory(self):
        path = Path(api._data_file_path("payments.json"))
        self.assertTrue(path.is_absolute())
        self.assertEqual(Path(api.__file__).resolve().parent / "data" / "payments.json", path)

    def test_load_json_file_reads_from_application_data_directory(self):
        expected = [{"payment_id": "P-test"}]
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            data_dir.mkdir()
            (data_dir / "payments.json").write_text(json.dumps(expected), encoding="utf-8")
            with patch.object(api, "DATA_DIR", str(data_dir)):
                self.assertEqual(expected, api.load_json_file("payments.json"))


if __name__ == "__main__":
    unittest.main()
