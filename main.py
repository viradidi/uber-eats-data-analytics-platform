from fastapi import FastAPI, Query, HTTPException, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from typing import Optional
from datetime import datetime, timezone
import json
import logging
import os
import random
import re
import tempfile
import time
import uuid


# =========================================================
# API CONFIGURATION
# =========================================================

API_KEY = os.getenv("API_KEY")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


# =========================================================
# FASTAPI APPLICATION
# =========================================================

app = FastAPI(
    title="Uber Eats Mock API",
    description="Mock REST API for Uber Eats Data Engineering Project",
    version="2.2"
)

api_logger = logging.getLogger("ubereats.api")
_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


@app.middleware("http")
async def request_observability(request: Request, call_next):
    """Emit one safe JSON event per request and propagate a correlation ID."""
    supplied_id = request.headers.get("X-Request-ID", "")
    request_id = (
        supplied_id
        if _REQUEST_ID_PATTERN.fullmatch(supplied_id)
        else uuid.uuid4().hex
    )
    request.state.request_id = request_id
    started_at = time.perf_counter()
    path = request.scope.get("path", "")

    try:
        response = await call_next(request)
    except Exception as exc:
        duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
        api_logger.error(json.dumps({
            "event": "http_request",
            "request_id": request_id,
            "method": request.method,
            "path": path,
            "status_code": 500,
            "duration_ms": duration_ms,
            "outcome": "unhandled_exception",
            "error_type": type(exc).__name__,
        }, separators=(",", ":")))
        response = JSONResponse(
            status_code=500,
            content={
                "detail": "Internal server error",
                "request_id": request_id,
            },
            headers={"X-Request-ID": request_id},
        )
        return response

    duration_ms = round((time.perf_counter() - started_at) * 1000, 3)
    response.headers["X-Request-ID"] = request_id
    api_logger.info(json.dumps({
        "event": "http_request",
        "request_id": request_id,
        "method": request.method,
        "path": path,
        "status_code": response.status_code,
        "duration_ms": duration_ms,
        "outcome": "completed",
    }, separators=(",", ":")))
    return response


# =========================================================
# API KEY SECURITY
# =========================================================

api_key_header = APIKeyHeader(
    name="X-API-Key",
    description="Enter your Uber Eats API key"
)


def verify_api_key(
    api_key: str = Depends(api_key_header)
):
    # Fail closed: never run with a public, hard-coded fallback key.
    if not API_KEY:
        raise HTTPException(
            status_code=503,
            detail="API authentication is not configured. Set API_KEY."
        )

    if api_key != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )
    return api_key


# =========================================================
# LOAD JSON FILE
# =========================================================

def _data_file_path(file_name):
    """Resolve source fixtures relative to this module, not the process CWD."""
    return os.path.join(DATA_DIR, file_name)


def load_json_file(file_name):
    file_path = _data_file_path(file_name)

    try:
        with open(
            file_path,
            "r",
            encoding="utf-8"
        ) as file:
            data = json.load(file)

    except FileNotFoundError:
        # Do not disclose server-side paths in API error responses.
        raise HTTPException(
            status_code=500,
            detail="Source data file is unavailable."
        )

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=500,
            detail="Source data file contains invalid JSON."
        )

    if not isinstance(data, list):
        raise HTTPException(
            status_code=500,
            detail="Source data file must contain a JSON array of records."
        )

    if any(not isinstance(record, dict) for record in data):
        raise HTTPException(
            status_code=500,
            detail="Source data file contains a record that is not a JSON object."
        )

    return data


# =========================================================
# TIMESTAMP HELPERS
# =========================================================

def parse_timestamp(value):
    """
    Convert an ISO-8601 timestamp into a timezone-aware UTC datetime.

    This handles both:
    - timezone-aware values such as 2026-09-01T12:20:00Z
    - timezone-naive values such as 2026-09-01T12:20:00

    Naive timestamps are treated as UTC so that API incremental
    filtering does not fail when comparing them with ADF timestamps.
    """
    try:
        if not value:
            return None

        parsed = datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.astimezone(timezone.utc)

    except (ValueError, TypeError):
        return None


def get_max_timestamp(file_name, timestamp_column):
    data = load_json_file(file_name)

    timestamps = []

    for record in data:
        value = record.get(timestamp_column)

        if not value:
            continue

        parsed = parse_timestamp(value)

        if parsed:
            timestamps.append(parsed)

    if not timestamps:
        return None

    max_timestamp = max(timestamps)

    return (
        max_timestamp
        .astimezone(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


# =========================================================
# GENERIC DATA PROCESSING
# =========================================================

def process_data(
    file_name,
    timestamp_column,
    page,
    page_size,
    updated_after: Optional[str] = None
):

    data = load_json_file(file_name)

    # -----------------------------------------------------
    # Incremental filtering
    # -----------------------------------------------------

    if updated_after:

        filter_date = parse_timestamp(updated_after)

        if not filter_date:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Invalid updated_after format. "
                    "Use ISO-8601 format, e.g. "
                    "2026-09-04T10:00:00Z"
                )
            )

        filtered_data = []

        for record in data:

            if timestamp_column not in record:
                continue

            record_date = parse_timestamp(
                record[timestamp_column]
            )

            if not record_date:
                continue

            if record_date > filter_date:
                filtered_data.append(record)

        data = filtered_data

    # -----------------------------------------------------
    # Total records
    # -----------------------------------------------------

    total_records = len(data)

    # -----------------------------------------------------
    # Pagination
    # -----------------------------------------------------

    start_index = (page - 1) * page_size
    end_index = start_index + page_size

    paginated_data = data[
        start_index:end_index
    ]

    # -----------------------------------------------------
    # Total pages
    # -----------------------------------------------------

    if total_records > 0:
        total_pages = (
            total_records
            + page_size
            - 1
        ) // page_size
    else:
        total_pages = 0

    # -----------------------------------------------------
    # API RESPONSE
    # -----------------------------------------------------

    return {
        "data": paginated_data,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_records": total_records,
            "total_pages": total_pages
        }
    }


# =========================================================
# SERVICE HEALTH
# =========================================================

HEALTH_SOURCE_FILES = {
    "payments": "payments.json",
    "deliveries": "deliveries.json",
    "ratings": "ratings.json",
    "order_status": "order_status.json",
}


@app.get("/health/live", tags=["Health"])
def health_liveness():
    """Confirm that the API process can respond to requests."""
    return {"status": "alive"}


@app.get("/health/ready", tags=["Health"])
def health_readiness():
    """Check authentication configuration and local source-fixture integrity."""
    checks = {
        "api_key_configured": "ok" if API_KEY else "not_configured",
    }

    for source_name, file_name in HEALTH_SOURCE_FILES.items():
        try:
            load_json_file(file_name)
            checks[source_name] = "ok"
        except (HTTPException, OSError, UnicodeError):
            # Return a bounded status rather than an internal path or exception.
            checks[source_name] = "unavailable_or_invalid"

    ready = all(status == "ok" for status in checks.values())
    response = JSONResponse(
        status_code=200 if ready else 503,
        content={
            "status": "ready" if ready else "not_ready",
            "checks": checks,
        },
    )
    response.headers["Cache-Control"] = "no-store"
    return response


# =========================================================
# HOME ENDPOINT
# =========================================================

@app.get("/")
def home():

    return {
        "message": "Uber Eats Mock API is running",
        "version": "2.2",
        "endpoints": [
            "/health/live",
            "/health/ready",
            "/payments",
            "/deliveries",
            "/ratings",
            "/order-status",
            "/watermarks",
            "/payments/generate-test-data"
        ]
    }


# =========================================================
# PAYMENTS API
# =========================================================

@app.get(
    "/payments",
    tags=["Payments"]
)
def get_payments(

    page: int = Query(
        1,
        ge=1,
        description="Page number"
    ),

    page_size: int = Query(
        5,
        ge=1,
        le=100,
        description="Number of records per page"
    ),

    updated_after: Optional[str] = Query(
        None,
        description="Return records after this timestamp"
    ),

    api_key: str = Depends(
        verify_api_key
    )
):

    return process_data(
        file_name="payments.json",
        timestamp_column="updated_at",
        page=page,
        page_size=page_size,
        updated_after=updated_after
    )


# =========================================================
# DELIVERIES API
# =========================================================

@app.get(
    "/deliveries",
    tags=["Deliveries"]
)
def get_deliveries(

    page: int = Query(
        1,
        ge=1,
        description="Page number"
    ),

    page_size: int = Query(
        5,
        ge=1,
        le=100,
        description="Number of records per page"
    ),

    updated_after: Optional[str] = Query(
        None,
        description="Return records after this timestamp"
    ),

    api_key: str = Depends(
        verify_api_key
    )
):

    return process_data(
        file_name="deliveries.json",
        timestamp_column="updated_at",
        page=page,
        page_size=page_size,
        updated_after=updated_after
    )


# =========================================================
# RATINGS API
# =========================================================

@app.get(
    "/ratings",
    tags=["Ratings"]
)
def get_ratings(

    page: int = Query(
        1,
        ge=1,
        description="Page number"
    ),

    page_size: int = Query(
        5,
        ge=1,
        le=100,
        description="Number of records per page"
    ),

    updated_after: Optional[str] = Query(
        None,
        description="Return records after this timestamp"
    ),

    api_key: str = Depends(
        verify_api_key
    )
):

    return process_data(
        file_name="ratings.json",
        timestamp_column="updated_at",
        page=page,
        page_size=page_size,
        updated_after=updated_after
    )


# =========================================================
# ORDER STATUS API
# =========================================================

@app.get(
    "/order-status",
    tags=["Order Status"]
)
def get_order_status(

    page: int = Query(
        1,
        ge=1,
        description="Page number"
    ),

    page_size: int = Query(
        5,
        ge=1,
        le=100,
        description="Number of records per page"
    ),

    updated_after: Optional[str] = Query(
        None,
        description="Return status events after this timestamp"
    ),

    api_key: str = Depends(
        verify_api_key
    )
):

    return process_data(
        file_name="order_status.json",
        timestamp_column="status_timestamp",
        page=page,
        page_size=page_size,
        updated_after=updated_after
    )


# =========================================================
# CENTRALIZED WATERMARK ENDPOINT
# =========================================================
#
# GET /watermarks
#
# The API dynamically calculates the current watermark for
# each source from its current JSON data.
#
# =========================================================

@app.get(
    "/watermarks",
    tags=["Watermarks"]
)
def get_watermarks(
    api_key: str = Depends(
        verify_api_key
    )
):

    return {
        "payments": get_max_timestamp(
            "payments.json",
            "updated_at"
        ),
        "deliveries": get_max_timestamp(
            "deliveries.json",
            "updated_at"
        ),
        "ratings": get_max_timestamp(
            "ratings.json",
            "updated_at"
        ),
        "order-status": get_max_timestamp(
            "order_status.json",
            "status_timestamp"
        )
    }


def _next_payment_number(records):
    """Return a numeric suffix above every existing P-prefixed payment ID."""
    highest = 1000
    for record in records:
        payment_id = str(record.get("payment_id", ""))
        if len(payment_id) < 2 or payment_id[0].upper() != "P":
            continue
        suffix = payment_id[1:]
        if suffix.isdigit():
            highest = max(highest, int(suffix))
    return highest + 1


def _write_json_atomically(file_path, data):
    """Write JSON to a sibling temporary file before replacing the target."""
    directory = os.path.dirname(os.path.abspath(file_path))
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=directory,
            prefix=".payments-",
            suffix=".tmp",
            delete=False,
        ) as file:
            temporary_path = file.name
            json.dump(data, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary_path, file_path)
    except OSError:
        if temporary_path and os.path.exists(temporary_path):
            try:
                os.remove(temporary_path)
            except OSError:
                pass
        raise


# =========================================================
# TEST DATA GENERATOR
# =========================================================
#
# POST /payments/generate-test-data
#
# This creates a new payment with the current UTC timestamp.
# It lets us demonstrate incremental loading without
# redeploying the API.
#
# =========================================================

@app.post(
    "/payments/generate-test-data",
    tags=["Test Data"]
)
def generate_payment_test_data(
    api_key: str = Depends(
        verify_api_key
    )
):

    data = load_json_file("payments.json")

    next_number = _next_payment_number(data)

    event_timestamp = (
        datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )
    new_payment = {
        "payment_id": f"P{next_number}",
        "order_id": next_number,
        "amount": round(
            random.uniform(10, 60),
            2
        ),
        "payment_method": random.choice(
            ["CARD", "UPI"]
        ),
        "payment_status": "COMPLETED",
        "payment_timestamp": event_timestamp,
        "updated_at": event_timestamp
    }

    data.append(new_payment)

    file_path = _data_file_path("payments.json")

    try:
        _write_json_atomically(file_path, data)

    except OSError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Unable to write test data: {exc}"
        )

    return {
        "message": "Test payment created",
        "record": new_payment
    }


# =========================================================
# APPLICATION STARTUP
# =========================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(
            os.getenv(
                "PORT",
                "10000"
            )
        )
    )
