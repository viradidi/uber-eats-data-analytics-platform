"""Behavioral tests for the mock API's timestamp and pagination contract.

These tests exercise Python API helpers with in-memory records. They do not require
Azure or an HTTP server and do not claim to validate deployed infrastructure.
"""
import unittest
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


if __name__ == "__main__":
    unittest.main()
