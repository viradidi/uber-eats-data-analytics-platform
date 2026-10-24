from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.security import APIKeyHeader
from typing import Optional
from datetime import datetime, timezone
import json
import os
import random


# =========================================================
# API CONFIGURATION
# =========================================================

API_KEY = os.getenv(
    "API_KEY",
    "ubereats-demo-key-123"
)


# =========================================================
# FASTAPI APPLICATION
# =========================================================

app = FastAPI(
    title="Uber Eats Mock API",
    description="Mock REST API for Uber Eats Data Engineering Project",
    version="2.1"
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
    if api_key != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )
    return api_key


# =========================================================
# LOAD JSON FILE
# =========================================================

def load_json_file(file_name):
    file_path = os.path.join(
        "data",
        file_name
    )

    try:
        with open(
            file_path,
            "r",
            encoding="utf-8"
        ) as file:
            return json.load(file)

    except FileNotFoundError:
        raise HTTPException(
            status_code=500,
            detail=f"File not found: {file_path}"
        )

    except json.JSONDecodeError:
        raise HTTPException(
            status_code=500,
            detail=f"Invalid JSON file: {file_path}"
        )


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
# HOME ENDPOINT
# =========================================================

@app.get("/")
def home():

    return {
        "message": "Uber Eats Mock API is running",
        "version": "2.1",
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

    next_number = 1001 + len(data)

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
        "updated_at": (
            datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z")
        )
    }

    data.append(new_payment)

    file_path = os.path.join(
        "data",
        "payments.json"
    )

    try:
        with open(
            file_path,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                data,
                file,
                indent=2
            )

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
