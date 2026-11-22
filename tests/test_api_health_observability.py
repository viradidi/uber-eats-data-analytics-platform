"""Tests for API health probes and request-level observability."""

import asyncio
import json
import unittest
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from starlette.requests import Request

import main as api


def make_request(path="/health/live", headers=None, query=b""):
    encoded_headers = [
        (name.lower().encode("latin-1"), value.encode("latin-1"))
        for name, value in (headers or {}).items()
    ]
    return Request({
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": query,
        "headers": encoded_headers,
        "server": ("testserver", 80),
        "client": ("testclient", 12345),
        "root_path": "",
    })


class ApiHealthProbeTests(unittest.TestCase):
    def test_liveness_probe_returns_alive_without_dependencies(self):
        self.assertEqual({"status": "alive"}, api.health_liveness())

    def test_readiness_returns_200_when_key_and_all_fixtures_are_valid(self):
        with patch.object(api, "API_KEY", "configured-test-key"), patch.object(
            api, "load_json_file", return_value=[]
        ) as loader:
            response = api.health_readiness()

        payload = json.loads(response.body)
        self.assertEqual(200, response.status_code)
        self.assertEqual("ready", payload["status"])
        self.assertEqual("ok", payload["checks"]["api_key_configured"])
        self.assertEqual(
            {"payments", "deliveries", "ratings", "order_status"},
            set(payload["checks"]) - {"api_key_configured"},
        )
        self.assertEqual(4, loader.call_count)
        self.assertEqual("no-store", response.headers["Cache-Control"])

    def test_readiness_returns_503_when_api_key_is_not_configured(self):
        with patch.object(api, "API_KEY", None), patch.object(
            api, "load_json_file", return_value=[]
        ):
            response = api.health_readiness()

        payload = json.loads(response.body)
        self.assertEqual(503, response.status_code)
        self.assertEqual("not_ready", payload["status"])
        self.assertEqual("not_configured", payload["checks"]["api_key_configured"])

    def test_readiness_returns_503_when_any_source_fixture_is_invalid(self):
        def load_fixture(file_name):
            if file_name == "payments.json":
                raise HTTPException(status_code=500, detail="internal path must not leak")
            return []

        with patch.object(api, "API_KEY", "configured-test-key"), patch.object(
            api, "load_json_file", side_effect=load_fixture
        ):
            response = api.health_readiness()

        payload = json.loads(response.body)
        self.assertEqual(503, response.status_code)
        self.assertEqual("unavailable_or_invalid", payload["checks"]["payments"])
        self.assertNotIn("internal path", response.body.decode("utf-8"))

    def test_health_routes_are_public_and_discoverable(self):
        routes = {
            route.path: route
            for route in api.app.routes
            if isinstance(route, APIRoute)
        }
        for path in ("/health/live", "/health/ready"):
            with self.subTest(path=path):
                self.assertIn(path, routes)
                self.assertEqual([], routes[path].dependant.dependencies)

        root = routes["/"].endpoint()
        self.assertIn("/health/live", root["endpoints"])
        self.assertIn("/health/ready", root["endpoints"])
        self.assertEqual("2.2", root["version"])


class RequestObservabilityTests(unittest.TestCase):
    def test_valid_caller_request_id_is_echoed_and_logged(self):
        request_id = "pipeline-run:abc-123"
        request = make_request(
            path="/payments",
            headers={"X-Request-ID": request_id},
            query=b"api_key=must-not-be-logged",
        )

        async def call_next(_request):
            return JSONResponse({"ok": True}, status_code=200)

        with self.assertLogs("ubereats.api", level="INFO") as captured:
            response = asyncio.run(api.request_observability(request, call_next))

        self.assertEqual(request_id, response.headers["X-Request-ID"])
        self.assertEqual(request_id, request.state.request_id)
        event = json.loads(captured.output[0].split(":", 2)[-1].strip())
        self.assertEqual("http_request", event["event"])
        self.assertEqual(request_id, event["request_id"])
        self.assertEqual("/payments", event["path"])
        self.assertEqual(200, event["status_code"])
        self.assertGreaterEqual(event["duration_ms"], 0)
        self.assertNotIn("must-not-be-logged", captured.output[0])
        self.assertNotIn("api_key", captured.output[0])

    def test_invalid_caller_request_id_is_replaced_with_uuid(self):
        request = make_request(headers={"X-Request-ID": "bad request\nforged"})

        async def call_next(_request):
            return JSONResponse({"ok": True})

        with self.assertLogs("ubereats.api", level="INFO"):
            response = asyncio.run(api.request_observability(request, call_next))

        generated_id = response.headers["X-Request-ID"]
        self.assertNotEqual("bad request\nforged", generated_id)
        self.assertEqual(32, len(generated_id))
        self.assertTrue(all(character in "0123456789abcdef" for character in generated_id))

    def test_unhandled_exception_is_sanitized_and_correlated(self):
        request = make_request(path="/payments")

        async def call_next(_request):
            raise RuntimeError("sensitive internal detail")

        with self.assertLogs("ubereats.api", level="ERROR") as captured:
            response = asyncio.run(api.request_observability(request, call_next))

        self.assertEqual(500, response.status_code)
        self.assertEqual(response.headers["X-Request-ID"], json.loads(response.body)["request_id"])
        self.assertNotIn("sensitive internal detail", response.body.decode("utf-8"))
        event = json.loads(captured.output[0].split(":", 2)[-1].strip())
        self.assertEqual("unhandled_exception", event["outcome"])
        self.assertEqual("RuntimeError", event["error_type"])


if __name__ == "__main__":
    unittest.main()
