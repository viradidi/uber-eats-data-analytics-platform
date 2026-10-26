"""Structural tests for the public query-parameter contract of the mock API."""

import unittest

import main as api
from fastapi.routing import APIRoute


class ApiQueryContractTests(unittest.TestCase):
    DATA_ROUTES = ("/payments", "/deliveries", "/ratings", "/order-status")

    @classmethod
    def setUpClass(cls):
        cls.routes = {
            route.path: route
            for route in api.app.routes
            if isinstance(route, APIRoute)
        }

    def test_incremental_data_routes_expose_bounded_pagination(self):
        for path in self.DATA_ROUTES:
            with self.subTest(path=path):
                self.assertIn(path, self.routes)
                params = {
                    parameter.name: parameter.field_info
                    for parameter in self.routes[path].dependant.query_params
                }
                self.assertIn("page", params)
                self.assertIn("page_size", params)
                self.assertEqual(1, params["page"].ge)
                self.assertEqual(1, params["page_size"].ge)
                self.assertEqual(100, params["page_size"].le)

    def test_incremental_data_routes_expose_updated_after_cursor(self):
        for path in self.DATA_ROUTES:
            with self.subTest(path=path):
                params = {
                    parameter.name: parameter
                    for parameter in self.routes[path].dependant.query_params
                }
                self.assertIn("updated_after", params)
                self.assertIsNone(params["updated_after"].required)


if __name__ == "__main__":
    unittest.main()
