from fastapi import FastAPI, Query, HTTPException, Depends, Request
from fastapi.security import APIKeyHeader
from typing import Optional
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import os
import random
import tempfile


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


def _format_utc_timestamp(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _cursor_value_key(value):
    """Order numeric IDs numerically and other IDs lexicographically."""
    if value is None or isinstance(value, bool):
        raise ValueError("Cursor ID must be a non-empty string or number.")
    text = str(value).strip()
    if not text:
        raise ValueError("Cursor ID must not be empty.")
    try:
        numeric_value = Decimal(text)
        if numeric_value.is_finite():
            return (0, numeric_value)
    except (InvalidOperation, ValueError):
        pass
    return (1, text)


def get_max_timestamp(file_name, timestamp_column):
    data = load_json_file(file_name)
    timestamps = [
        parsed for record in data
        if (parsed := parse_timestamp(record.get(timestamp_column))) is not None
    ]
    return _format_utc_timestamp(max(timestamps)) if timestamps else None


def get_max_cursor(file_name, timestamp_column, cursor_column):
    """Return the maximum deterministic (UTC timestamp, stable ID) cursor."""
    data = load_json_file(file_name)
    candidates = []
    for record in data:
        parsed = parse_timestamp(record.get(timestamp_column))
        cursor_value = record.get(cursor_column)
        if parsed is None or cursor_value is None:
            continue
        try:
            cursor_key = _cursor_value_key(cursor_value)
        except ValueError:
            continue
        candidates.append((parsed, cursor_key, cursor_value))
    if not candidates:
        return {"updated_at": None, "id": None}
    timestamp_value, _, cursor_value = max(candidates, key=lambda item: (item[0], item[1]))
    return {"updated_at": _format_utc_timestamp(timestamp_value), "id": str(cursor_value)}


# =========================================================
# GENERIC DATA PROCESSING
# =========================================================

def process_data(
    file_name,
    timestamp_column,
    page,
    page_size,
    updated_after: Optional[str] = None,
    updated_after_id: Optional[str] = None,
    updated_until: Optional[str] = None,
    updated_until_id: Optional[str] = None,
    cursor_column: Optional[str] = None,
    request: Optional[Request] = None,
):
    data = load_json_file(file_name)
    if updated_after_id is not None and not updated_after:
        raise HTTPException(status_code=400, detail="updated_after_id requires updated_after.")
    if (updated_until is None) != (updated_until_id is None):
        raise HTTPException(
            status_code=400,
            detail="updated_until and updated_until_id must be supplied together.",
        )

    filter_date = None
    upper_date = None
    upper_id_key = None
    filter_id_key = None
    if updated_after:
        filter_date = parse_timestamp(updated_after)
        if filter_date is None:
            raise HTTPException(status_code=400, detail="Invalid updated_after format. Use an ISO-8601 timestamp.")
        if updated_after_id is not None:
            if not cursor_column:
                raise HTTPException(status_code=400, detail="A stable cursor column is not configured for this endpoint.")
            try:
                filter_id_key = _cursor_value_key(updated_after_id)
            except ValueError:
                raise HTTPException(status_code=400, detail="updated_after_id must be a non-empty string or number.")

    if updated_until is not None:
        if not cursor_column:
            raise HTTPException(
                status_code=400,
                detail="A stable cursor column is not configured for this endpoint.",
            )
        upper_date = parse_timestamp(updated_until)
        if upper_date is None:
            raise HTTPException(status_code=400, detail="Invalid updated_until format. Use an ISO-8601 timestamp.")
        try:
            upper_id_key = _cursor_value_key(updated_until_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="updated_until_id must be a non-empty string or number.")

    if cursor_column:
        ordered_records = []
        for record in data:
            record_date = parse_timestamp(record.get(timestamp_column))
            cursor_value = record.get(cursor_column)
            if record_date is None or cursor_value is None:
                raise HTTPException(
                    status_code=500,
                    detail=f"Source record is missing a valid {timestamp_column} or {cursor_column} cursor value.",
                )
            try:
                cursor_key = _cursor_value_key(cursor_value)
            except ValueError:
                raise HTTPException(status_code=500, detail=f"Source record contains an invalid {cursor_column} cursor value.")
            ordered_records.append((record_date, cursor_key, record))
        ordered_records.sort(key=lambda item: (item[0], item[1]))

        if filter_date is not None:
            if filter_id_key is None:
                # Preserve timestamp-only behavior for older clients.
                ordered_records = [item for item in ordered_records if item[0] > filter_date]
            else:
                ordered_records = [
                    item for item in ordered_records
                    if item[0] > filter_date or (item[0] == filter_date and item[1] > filter_id_key)
                ]
        if upper_date is not None:
            ordered_records = [
                item for item in ordered_records
                if item[0] < upper_date or (item[0] == upper_date and item[1] <= upper_id_key)
            ]
        data = [item[2] for item in ordered_records]
    elif filter_date is not None:
        data = [
            record for record in data
            if (
                (record_date := parse_timestamp(record.get(timestamp_column))) is not None
                and record_date > filter_date
            )
        ]

    total_records = len(data)
    start_index = (page - 1) * page_size
    end_index = start_index + page_size
    paginated_data = data[start_index:end_index]
    total_pages = (total_records + page_size - 1) // page_size if total_records else 0

    next_url = None
    if cursor_column and request is not None and end_index < total_records and paginated_data:
        last_record = paginated_data[-1]
        next_url = str(
            request.url.include_query_params(
                updated_after=_format_utc_timestamp(parse_timestamp(last_record[timestamp_column])),
                updated_after_id=str(last_record[cursor_column]),
                page=1,
            )
        )
    return {
        "data": paginated_data,
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_records": total_records,
            "total_pages": total_pages,
            "next_url": next_url,
        },
    }


# =========================================================
# HOME ENDPOINT
# =========================================================

@app.get("/")
def home():

    return {
        "message": "Uber Eats Mock API is running",
        "version": "2.2",
        "endpoints": [
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

    request: Request,

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

    updated_after_id: Optional[str] = Query(
        None,
        description="Stable source ID paired with updated_after"
    ),

    updated_until: Optional[str] = Query(
        None,
        description="Inclusive upper timestamp captured before the ingestion copy"
    ),

    updated_until_id: Optional[str] = Query(
        None,
        description="Stable source ID paired with updated_until"
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
        updated_after=updated_after,
        updated_after_id=updated_after_id,
        updated_until=updated_until,
        updated_until_id=updated_until_id,
        cursor_column="payment_id",
        request=request,
    )


# =========================================================
# DELIVERIES API
# =========================================================

@app.get(
    "/deliveries",
    tags=["Deliveries"]
)
def get_deliveries(

    request: Request,

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

    updated_after_id: Optional[str] = Query(
        None,
        description="Stable source ID paired with updated_after"
    ),

    updated_until: Optional[str] = Query(
        None,
        description="Inclusive upper timestamp captured before the ingestion copy"
    ),

    updated_until_id: Optional[str] = Query(
        None,
        description="Stable source ID paired with updated_until"
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
        updated_after=updated_after,
        updated_after_id=updated_after_id,
        updated_until=updated_until,
        updated_until_id=updated_until_id,
        cursor_column="delivery_id",
        request=request,
    )


# =========================================================
# RATINGS API
# =========================================================

@app.get(
    "/ratings",
    tags=["Ratings"]
)
def get_ratings(

    request: Request,

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

    updated_after_id: Optional[str] = Query(
        None,
        description="Stable source ID paired with updated_after"
    ),

    updated_until: Optional[str] = Query(
        None,
        description="Inclusive upper timestamp captured before the ingestion copy"
    ),

    updated_until_id: Optional[str] = Query(
        None,
        description="Stable source ID paired with updated_until"
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
        updated_after=updated_after,
        updated_after_id=updated_after_id,
        updated_until=updated_until,
        updated_until_id=updated_until_id,
        cursor_column="rating_id",
        request=request,
    )


# =========================================================
# ORDER STATUS API
# =========================================================

@app.get(
    "/order-status",
    tags=["Order Status"]
)
def get_order_status(

    request: Request,

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

    updated_after_id: Optional[str] = Query(
        None,
        description="Stable source ID paired with updated_after"
    ),

    updated_until: Optional[str] = Query(
        None,
        description="Inclusive upper timestamp captured before the ingestion copy"
    ),

    updated_until_id: Optional[str] = Query(
        None,
        description="Stable source ID paired with updated_until"
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
        updated_after=updated_after,
        updated_after_id=updated_after_id,
        updated_until=updated_until,
        updated_until_id=updated_until_id,
        cursor_column="order_id",
        request=request,
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

    cursor = {
        "payments": get_max_cursor("payments.json", "updated_at", "payment_id"),
        "deliveries": get_max_cursor("deliveries.json", "updated_at", "delivery_id"),
        "ratings": get_max_cursor("ratings.json", "updated_at", "rating_id"),
        "order-status": get_max_cursor("order_status.json", "status_timestamp", "order_id"),
    }
    return {
        "payments": cursor["payments"]["updated_at"],
        "deliveries": cursor["deliveries"]["updated_at"],
        "ratings": cursor["ratings"]["updated_at"],
        "order-status": cursor["order-status"]["updated_at"],
        "cursor": cursor,
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
