"""Structural tests for the public query-parameter contract of the mock API."""

import unittest

import main as api


class ApiQueryContractTests(unittest.TestCase):
    DATA_ROUTES = ("/payments", "/deliveries", "/ratings", "/order-status")

    @classmethod
    def setUpClass(cls):
        cls.openapi = api.app.openapi()

    def _query_parameters(self, path):
        operation = self.openapi["paths"][path]["get"]
        return {
            parameter["name"]: parameter
            for parameter in operation.get("parameters", [])
            if parameter.get("in") == "query"
        }

    def test_incremental_data_routes_expose_bounded_pagination(self):
        for path in self.DATA_ROUTES:
            with self.subTest(path=path):
                params = self._query_parameters(path)
                self.assertIn("page", params)
                self.assertIn("page_size", params)
                self.assertEqual(1, params["page"]["schema"]["minimum"])
                self.assertEqual(1, params["page_size"]["schema"]["minimum"])
                self.assertEqual(100, params["page_size"]["schema"]["maximum"])

    def test_incremental_data_routes_expose_optional_updated_after_cursor(self):
        for path in self.DATA_ROUTES:
            with self.subTest(path=path):
                params = self._query_parameters(path)
                self.assertIn("updated_after", params)
                self.assertFalse(params["updated_after"].get("required", False))


if __name__ == "__main__":
    unittest.main()
