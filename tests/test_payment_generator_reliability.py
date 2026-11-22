"""Reliability tests for the local payment test-data generator."""

import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

import main as api


class PaymentGeneratorReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp_dir.name)
        self.payments_path = self.data_dir / "payments.json"
        self.payments_path.write_text(
            json.dumps([{"payment_id": "P1008", "order_id": 5008}]),
            encoding="utf-8",
        )
        self.data_dir_patcher = patch.object(api, "DATA_DIR", str(self.data_dir))
        self.data_dir_patcher.start()

    def tearDown(self):
        self.data_dir_patcher.stop()
        self.temp_dir.cleanup()

    def generate(self, key=None):
        return api.generate_payment_test_data(
            api_key="local-test-key",
            idempotency_key=key,
        )

    def read_payments(self):
        return json.loads(self.payments_path.read_text(encoding="utf-8"))

    def test_repeated_idempotency_key_returns_original_payment_without_append(self):
        first = self.generate("payment-request-001")
        replay = self.generate("payment-request-001")

        self.assertFalse(first["replayed"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(first["record"], replay["record"])
        self.assertEqual(2, len(self.read_payments()))

    def test_concurrent_requests_with_same_key_create_one_payment(self):
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(
                executor.map(
                    lambda _: self.generate("concurrent-payment-request"),
                    range(12),
                )
            )

        self.assertEqual(1, sum(not result["replayed"] for result in results))
        self.assertEqual(1, len({result["record"]["payment_id"] for result in results}))
        self.assertEqual(2, len(self.read_payments()))

    def test_concurrent_distinct_requests_do_not_lose_updates_or_reuse_ids(self):
        request_count = 12
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(
                executor.map(
                    lambda index: self.generate(f"payment-request-{index:03d}"),
                    range(request_count),
                )
            )

        generated_ids = [result["record"]["payment_id"] for result in results]
        self.assertEqual(request_count, len(set(generated_ids)))
        self.assertEqual(request_count + 1, len(self.read_payments()))

    def test_retry_recovers_when_process_stops_after_journal_write(self):
        original_write = api._write_json_atomically

        def fail_payment_fixture_once(file_path, payload):
            if Path(file_path).name == "payments.json":
                raise OSError("simulated interruption before payment fixture replace")
            return original_write(file_path, payload)

        with patch.object(
            api,
            "_write_json_atomically",
            side_effect=fail_payment_fixture_once,
        ):
            with self.assertRaises(HTTPException) as raised:
                self.generate("recoverable-payment-request")

        self.assertEqual(500, raised.exception.status_code)
        self.assertEqual(1, len(self.read_payments()))

        replay = self.generate("recoverable-payment-request")

        self.assertTrue(replay["replayed"])
        self.assertEqual(2, len(self.read_payments()))
        self.assertEqual(replay["record"], self.read_payments()[-1])

    def test_idempotency_key_must_not_be_blank(self):
        with self.assertRaises(HTTPException) as raised:
            self.generate("   ")
        self.assertEqual(400, raised.exception.status_code)
        self.assertEqual(1, len(self.read_payments()))

    def test_idempotency_key_is_limited_to_255_characters(self):
        with self.assertRaises(HTTPException) as raised:
            self.generate("k" * 256)
        self.assertEqual(400, raised.exception.status_code)
        self.assertEqual(1, len(self.read_payments()))

    def test_openapi_exposes_optional_idempotency_header(self):
        operation = api.app.openapi()["paths"][
            "/payments/generate-test-data"
        ]["post"]
        parameters = {
            parameter["name"]: parameter
            for parameter in operation.get("parameters", [])
        }

        self.assertIn("Idempotency-Key", parameters)
        self.assertEqual("header", parameters["Idempotency-Key"]["in"])
        self.assertFalse(parameters["Idempotency-Key"].get("required", False))

    def test_ledger_conflict_is_reported_instead_of_overwriting_payment(self):
        existing_payment = {
            "payment_id": "P1009",
            "order_id": 1009,
            "amount": 25.0,
        }
        self.payments_path.write_text(
            json.dumps(
                [
                    {"payment_id": "P1008", "order_id": 5008},
                    dict(existing_payment, amount=99.0),
                ]
            ),
            encoding="utf-8",
        )
        (self.data_dir / "payments.idempotency.json").write_text(
            json.dumps({"conflicting-key": existing_payment}),
            encoding="utf-8",
        )

        with self.assertRaises(HTTPException) as raised:
            self.generate("conflicting-key")

        self.assertEqual(409, raised.exception.status_code)
        self.assertEqual(99.0, self.read_payments()[-1]["amount"])


if __name__ == "__main__":
    unittest.main()
