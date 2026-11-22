from contextlib import contextmanager
from fastapi import FastAPI, Query, HTTPException, Depends, Header
from fastapi.security import APIKeyHeader
from typing import Annotated, Optional
from datetime import datetime, timezone
import errno
import json
import os
import random
import tempfile
import threading
import time


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


# The thread lock protects workers in this process. The OS-level lock below
# also coordinates separate API processes sharing the same local data folder.
_PAYMENTS_THREAD_LOCK = threading.Lock()


@contextmanager
def _payments_write_lock(lock_directory=None):
    """Serialize payment fixture writes across threads and local processes."""
    lock_directory = lock_directory or DATA_DIR
    os.makedirs(lock_directory, exist_ok=True)
    lock_path = os.path.join(lock_directory, ".payments.lock")

    with _PAYMENTS_THREAD_LOCK:
        with open(lock_path, "a+b") as lock_file:
            if os.name == "nt":
                import msvcrt

                lock_file.seek(0, os.SEEK_END)
                if lock_file.tell() == 0:
                    lock_file.write(b"\0")
                    lock_file.flush()

                while True:
                    lock_file.seek(0)
                    try:
                        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                        break
                    except OSError as exc:
                        if exc.errno not in (errno.EACCES, errno.EDEADLK):
                            raise
                        time.sleep(0.05)

                try:
                    yield
                finally:
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _write_json_atomically(file_path, data):
    """Persist JSON using a sibling temp file and atomic replacement."""
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

        # Persist the directory entry on POSIX filesystems where directory
        # fsync is supported. Atomic replace still protects readers if this
        # optional durability step is unavailable.
        if os.name != "nt":
            try:
                directory_fd = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except OSError:
                pass
    except OSError:
        if temporary_path and os.path.exists(temporary_path):
            try:
                os.remove(temporary_path)
            except OSError:
                pass
        raise


def _load_payment_idempotency_ledger(ledger_path=None):
    """Load the local request-key journal; an absent journal is empty state."""
    ledger_path = ledger_path or _data_file_path("payments.idempotency.json")
    try:
        with open(ledger_path, "r", encoding="utf-8") as file:
            ledger = json.load(file)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=500,
            detail="Payment idempotency state is invalid and requires recovery.",
        )

    if not isinstance(ledger, dict) or any(
        not isinstance(key, str) or not isinstance(value, dict)
        for key, value in ledger.items()
    ):
        raise HTTPException(
            status_code=500,
            detail="Payment idempotency state has an invalid structure.",
        )
    return ledger


# =========================================================
# TEST DATA GENERATOR
# =========================================================
#
# POST /payments/generate-test-data
#
# Optional Idempotency-Key makes client retries replay-safe.
# A local lock serializes cooperating API workers that share DATA_DIR.
# =========================================================


@app.post(
    "/payments/generate-test-data",
    tags=["Test Data"],
)
def generate_payment_test_data(
    api_key: str = Depends(verify_api_key),
    idempotency_key: Annotated[
        Optional[str],
        Header(
            alias="Idempotency-Key",
            description=(
                "Optional client-generated key (1–255 characters). Reusing the "
                "same key returns the original generated payment."
            ),
        ),
    ] = None,
):
    request_key = idempotency_key.strip() if idempotency_key is not None else None
    if request_key == "":
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key must not be empty.",
        )
    if request_key is not None and len(request_key) > 255:
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key must not exceed 255 characters.",
        )

    payment_path = _data_file_path("payments.json")
    payment_directory = os.path.dirname(os.path.abspath(payment_path))
    ledger_path = os.path.join(payment_directory, "payments.idempotency.json")

    try:
        with _payments_write_lock(payment_directory):
            data = load_json_file("payments.json")
            ledger = _load_payment_idempotency_ledger(ledger_path)

            if request_key is not None and request_key in ledger:
                original_payment = ledger[request_key]
                payment_id = original_payment.get("payment_id")
                if not payment_id:
                    raise HTTPException(
                        status_code=500,
                        detail="Stored payment idempotency record is invalid.",
                    )

                existing = next(
                    (
                        record
                        for record in data
                        if record.get("payment_id") == payment_id
                    ),
                    None,
                )

                if existing is None:
                    # Recover a process interruption after the journal write
                    # but before the payments fixture was atomically replaced.
                    data.append(original_payment)
                    _write_json_atomically(payment_path, data)
                elif existing != original_payment:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "The stored Idempotency-Key conflicts with the "
                            "current payment fixture. Manual reconciliation is required."
                        ),
                    )

                return {
                    "message": "Payment already created for this Idempotency-Key",
                    "record": original_payment,
                    "replayed": True,
                }

            next_number = _next_payment_number(data)
            event_timestamp = (
                datetime.now(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
            new_payment = {
                "payment_id": f"P{next_number}",
                "order_id": next_number,
                "amount": round(random.uniform(10, 60), 2),
                "payment_method": random.choice(["CARD", "UPI"]),
                "payment_status": "COMPLETED",
                "payment_timestamp": event_timestamp,
                "updated_at": event_timestamp,
            }

            if request_key is not None:
                # Write the idempotency journal first. If the process stops
                # before the payments file is replaced, a retry can complete
                # the append using this exact original payment record.
                ledger[request_key] = new_payment
                _write_json_atomically(ledger_path, ledger)

            data.append(new_payment)
            _write_json_atomically(payment_path, data)

            return {
                "message": "Test payment created",
                "record": new_payment,
                "replayed": False,
            }

    except OSError:
        # Do not return filesystem paths or low-level OS details to API clients.
        raise HTTPException(
            status_code=500,
            detail=(
                "Unable to persist the generated payment. "
                "Retry using the same Idempotency-Key."
            ),
        )


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
