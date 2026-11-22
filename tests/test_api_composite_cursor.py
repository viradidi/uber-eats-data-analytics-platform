"""Tie-safe API cursor and exported ADF pagination contract tests."""
import json
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch
from fastapi import HTTPException
from starlette.datastructures import URL
import main as api

ROOT = Path(__file__).resolve().parents[1]

class RequestStub:
    def __init__(self, url):
        self.url = URL(url)

class CompositeCursorTests(unittest.TestCase):
    def test_equal_timestamp_records_resume_after_stable_numeric_id(self):
        rows = [
            {"payment_id": 1003, "updated_at": "2026-11-01T12:00:00Z"},
            {"payment_id": 1001, "updated_at": "2026-11-01T12:00:00+00:00"},
            {"payment_id": 1002, "updated_at": "2026-11-01T12:00:00Z"},
            {"payment_id": 1004, "updated_at": "2026-11-01T12:00:01Z"},
        ]
        with patch.object(api, "load_json_file", return_value=rows):
            result = api.process_data("payments.json", "updated_at", 1, 10,
                updated_after="2026-11-01T12:00:00Z", updated_after_id="1001",
                cursor_column="payment_id")
        self.assertEqual([1002, 1003, 1004], [x["payment_id"] for x in result["data"]])

    def test_numeric_ids_are_ordered_numerically(self):
        rows = [
            {"delivery_id": 10, "updated_at": "2026-11-01T12:00:00Z"},
            {"delivery_id": 9, "updated_at": "2026-11-01T12:00:00Z"},
            {"delivery_id": 100, "updated_at": "2026-11-01T12:00:00Z"},
        ]
        with patch.object(api, "load_json_file", return_value=rows):
            result = api.process_data("deliveries.json", "updated_at", 1, 10,
                updated_after="2026-11-01T12:00:00Z", updated_after_id="9",
                cursor_column="delivery_id")
        self.assertEqual([10, 100], [x["delivery_id"] for x in result["data"]])

    def test_prefixed_numeric_ids_are_ordered_by_numeric_suffix(self):
        rows = [
            {"payment_id": "P10000", "updated_at": "2026-11-01T12:00:00Z"},
            {"payment_id": "P9999", "updated_at": "2026-11-01T12:00:00Z"},
            {"payment_id": "P10001", "updated_at": "2026-11-01T12:00:00Z"},
        ]
        with patch.object(api, "load_json_file", return_value=rows):
            result = api.process_data(
                "payments.json", "updated_at", 1, 10,
                updated_after="2026-11-01T12:00:00Z",
                updated_after_id="P9999", cursor_column="payment_id",
            )
        self.assertEqual(["P10000", "P10001"], [x["payment_id"] for x in result["data"]])

    def test_string_ids_are_ordered_lexicographically(self):
        rows = [
            {"payment_id": "P1010", "updated_at": "2026-11-01T12:00:00Z"},
            {"payment_id": "P1009", "updated_at": "2026-11-01T12:00:00Z"},
            {"payment_id": "P1011", "updated_at": "2026-11-01T12:00:00Z"},
        ]
        with patch.object(api, "load_json_file", return_value=rows):
            result = api.process_data("payments.json", "updated_at", 1, 10,
                updated_after="2026-11-01T12:00:00Z", updated_after_id="P1009",
                cursor_column="payment_id")
        self.assertEqual(["P1010", "P1011"], [x["payment_id"] for x in result["data"]])

    def test_timestamp_only_filter_remains_backward_compatible(self):
        rows = [
            {"rating_id": 1, "updated_at": "2026-11-01T12:00:00Z"},
            {"rating_id": 2, "updated_at": "2026-11-01T12:00:01Z"},
        ]
        with patch.object(api, "load_json_file", return_value=rows):
            result = api.process_data("ratings.json", "updated_at", 1, 10,
                updated_after="2026-11-01T12:00:00Z", cursor_column="rating_id")
        self.assertEqual([2], [x["rating_id"] for x in result["data"]])

    def test_id_without_timestamp_is_rejected(self):
        with patch.object(api, "load_json_file", return_value=[]):
            with self.assertRaises(HTTPException) as raised:
                api.process_data("payments.json", "updated_at", 1, 10,
                    updated_after_id="1001", cursor_column="payment_id")
        self.assertEqual(400, raised.exception.status_code)

    def test_missing_cursor_values_fail_closed(self):
        rows = [
            {"payment_id": 1, "updated_at": "2026-11-01T12:00:00Z"},
            {"updated_at": "2026-11-01T12:00:01Z"},
        ]
        with patch.object(api, "load_json_file", return_value=rows):
            with self.assertRaises(HTTPException) as raised:
                api.process_data("payments.json", "updated_at", 1, 10,
                    cursor_column="payment_id")
        self.assertEqual(500, raised.exception.status_code)

    def test_next_url_resumes_after_last_timestamp_and_id(self):
        rows = [{"rating_id": i, "updated_at": "2026-11-01T12:00:00Z"} for i in (1,2,3,4,5)]
        request = RequestStub("https://api.example.test/ratings?page_size=2")
        with patch.object(api, "load_json_file", return_value=rows):
            first = api.process_data("ratings.json", "updated_at", 1, 2,
                cursor_column="rating_id", request=request)
        query = parse_qs(urlparse(first["pagination"]["next_url"]).query)
        self.assertEqual(["2026-11-01T12:00:00Z"], query["updated_after"])
        self.assertEqual(["2"], query["updated_after_id"])
        with patch.object(api, "load_json_file", return_value=rows):
            second = api.process_data("ratings.json", "updated_at", 1, 2,
                updated_after=query["updated_after"][0],
                updated_after_id=query["updated_after_id"][0], cursor_column="rating_id")
        self.assertEqual([3,4], [x["rating_id"] for x in second["data"]])

    def test_inclusive_upper_cursor_bounds_a_run_snapshot(self):
        rows = [
            {"payment_id": 1, "updated_at": "2026-11-01T12:00:00Z"},
            {"payment_id": 2, "updated_at": "2026-11-01T12:00:00Z"},
            {"payment_id": 3, "updated_at": "2026-11-01T12:00:01Z"},
            {"payment_id": 4, "updated_at": "2026-11-01T12:00:02Z"},
        ]
        with patch.object(api, "load_json_file", return_value=rows):
            result = api.process_data(
                "payments.json", "updated_at", 1, 20,
                updated_after="2026-11-01T12:00:00Z", updated_after_id="1",
                updated_until="2026-11-01T12:00:01Z", updated_until_id="3",
                cursor_column="payment_id",
            )
        self.assertEqual([2, 3], [row["payment_id"] for row in result["data"]])

    def test_upper_cursor_pair_must_be_complete(self):
        with patch.object(api, "load_json_file", return_value=[]):
            with self.assertRaises(HTTPException) as raised:
                api.process_data(
                    "payments.json", "updated_at", 1, 10,
                    updated_until="2026-11-01T12:00:00Z",
                    cursor_column="payment_id",
                )
        self.assertEqual(400, raised.exception.status_code)

    def test_next_url_preserves_upper_snapshot_bound(self):
        rows = [{"rating_id": i, "updated_at": "2026-11-01T12:00:00Z"} for i in (1,2,3)]
        request = RequestStub(
            "https://api.example.test/ratings?page_size=1&updated_until=2026-11-01T13%3A00%3A00Z&updated_until_id=9"
        )
        with patch.object(api, "load_json_file", return_value=rows):
            result = api.process_data(
                "ratings.json", "updated_at", 1, 1, cursor_column="rating_id",
                request=request, updated_until="2026-11-01T13:00:00Z", updated_until_id="9",
            )
        query = parse_qs(urlparse(result["pagination"]["next_url"]).query)
        self.assertEqual(["2026-11-01T13:00:00Z"], query["updated_until"])
        self.assertEqual(["9"], query["updated_until_id"])

    def test_max_cursor_uses_id_to_break_timestamp_ties(self):
        rows = [
            {"order_id": 2002, "status_timestamp": "2026-11-01T12:00:00Z"},
            {"order_id": 2001, "status_timestamp": "2026-11-01T12:00:00+00:00"},
        ]
        with patch.object(api, "load_json_file", return_value=rows):
            cursor = api.get_max_cursor("order_status.json", "status_timestamp", "order_id")
        self.assertEqual("2002", cursor["id"])

    def test_watermark_response_keeps_legacy_fields_and_adds_cursor(self):
        rows = {
            "payments.json":[{"payment_id":1002,"updated_at":"2026-11-01T12:00:00Z"}],
            "deliveries.json":[{"delivery_id":3001,"updated_at":"2026-11-01T13:00:00Z"}],
            "ratings.json":[{"rating_id":4001,"updated_at":"2026-11-01T14:00:00Z"}],
            "order_status.json":[{"order_id":5001,"status_timestamp":"2026-11-01T15:00:00Z"}],
        }
        with patch.object(api, "load_json_file", side_effect=lambda name: rows[name]):
            result=api.get_watermarks(api_key="test-key")
        self.assertEqual("2026-11-01T12:00:00Z", result["payments"])
        self.assertEqual("1002", result["cursor"]["payments"]["id"])

    def test_adf_contract_uses_id_checkpoint_and_json_next_link(self):
        dataset=json.loads((ROOT/"dataset/Ds_rest_src.json").read_text(encoding="utf-8"))
        params=dataset["properties"]["parameters"]
        self.assertIn("p_watermark",params)
        self.assertIn("p_watermark_id",params)
        self.assertIn("p_upper_watermark",params)
        self.assertIn("p_upper_watermark_id",params)
        relative=dataset["properties"]["typeProperties"]["relativeUrl"]["value"]
        for token in ("dataset().p_watermark", "dataset().p_watermark_id",
                      "dataset().p_upper_watermark", "dataset().p_upper_watermark_id",
                      "uriComponent("):
            self.assertIn(token, relative)

        pipeline=json.loads((ROOT/"pipeline/api_src_inital load.json").read_text(encoding="utf-8"))
        loop=next(a for a in pipeline["properties"]["activities"] if a["type"]=="ForEach")
        acts={a["name"]:a for a in loop["typeProperties"]["activities"]}
        copy=acts["Api_migration"]
        self.assertEqual("$.pagination.next_url",copy["typeProperties"]["source"]["paginationRules"]["AbsoluteUrl"])
        self.assertIn("last_watermark_id",copy["inputs"][0]["parameters"]["p_watermark_id"]["value"])
        self.assertIn("Web_Get_Api_Watermarks",copy["inputs"][0]["parameters"]["p_upper_watermark"]["value"])
        self.assertIn("Web_Get_Api_Watermarks",copy["inputs"][0]["parameters"]["p_upper_watermark_id"]["value"])

        # The snapshot high watermark must be captured before copy and persisted only after Silver succeeds.
        self.assertEqual("Web_Get_Api_Watermarks",copy["dependsOn"][0]["activity"])
        self.assertEqual("get_keyvalut_api _key",acts["Web_Get_Api_Watermarks"]["dependsOn"][0]["activity"])
        self.assertEqual("Api_migration",acts["Job_api"]["dependsOn"][0]["activity"])
        self.assertEqual("Job_api",acts["Script_update_api_control"]["dependsOn"][0]["activity"])
        update_sql=acts["Script_update_api_control"]["typeProperties"]["scripts"][0]["text"]["value"]
        self.assertIn(".output.cursor[item().endpoint_name].updated_at",update_sql)
        self.assertIn(".output.cursor[item().endpoint_name].id",update_sql)

    def test_sql_migration_adds_nullable_id_without_resetting_rows(self):
        path=ROOT/"sql/migrations/20261122_add_api_composite_cursor.sql"
        self.assertTrue(path.is_file())
        sql=path.read_text(encoding="utf-8")
        self.assertIn("ADD last_watermark_id nvarchar(256) NULL",sql)
        self.assertIn("legacy_rows_requiring_one_full_snapshot_replay",sql)
        self.assertNotIn("DELETE FROM dbo.api_pipeline_control",sql)

if __name__ == "__main__":
    unittest.main()
