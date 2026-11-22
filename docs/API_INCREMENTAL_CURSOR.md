# API Incremental Cursor and Pagination

## Composite cursor contract

Each endpoint uses a deterministic timestamp and stable-ID tuple:

| Endpoint | Timestamp field | Stable identifier |
|---|---|---|
| Payments | updated_at | payment_id |
| Deliveries | updated_at | delivery_id |
| Ratings | updated_at | rating_id |
| Order status | status_timestamp | order_id |

The incremental predicate is:

    (timestamp > checkpoint_timestamp)
    OR (timestamp = checkpoint_timestamp AND stable_id > checkpoint_id)

Timestamps are normalized to UTC. Numeric IDs are ordered numerically and non-numeric IDs lexicographically. The order-status Silver event key remains (order_id, status_timestamp); conflicting payloads at that key continue to fail existing Silver validation.

## Request and response

Example:

    GET /payments?page_size=100&updated_after=2026-11-01T12%3A00%3A00Z&updated_after_id=1002

When another page exists, pagination.next_url contains an absolute URL carrying the timestamp and ID of the last returned record. The next request resumes strictly after that tuple. The final page returns next_url: null. Timestamp-only updated_after remains supported for older clients.

## ADF integration

Ds_rest_src receives the previous timestamp and ID from dbo.api_pipeline_control. If either value is missing, both parameters are omitted and the API returns a full snapshot. The REST Copy activity follows $.pagination.next_url using the AbsoluteUrl rule with a maximum of 1,000 requests.

The pipeline updates the composite checkpoint only after the endpoint's Silver dispatcher succeeds and the subsequent watermark request completes. The watermark response retains its timestamp-only fields and adds a cursor object.

## Deployment order

1. Back up and inspect the SQL control database.
2. Apply sql/migrations/20261122_add_api_composite_cursor.sql to the database containing dbo.api_pipeline_control.
3. Publish the updated Ds_rest_src dataset and api_src_inital load pipeline.
4. Deploy the API version that returns pagination.next_url and nested cursor metadata.
5. Run API ingestion. Old timestamp-only control rows have a NULL ID and intentionally replay one full snapshot; existing Silver replay deduplication absorbs repeated rows.
6. Verify both cursor fields are populated for active API controls and all four Silver tasks succeed.

Do not publish the ADF pagination contract before deploying the new API.

## Correctness assumptions

- Every API source row must have a parseable timestamp and non-null stable ID. The API fails closed if a row cannot be placed in the cursor order.
- A composite cursor resolves timestamp ties but does not detect hard deletes.
- A row becoming visible later with a timestamp older than the committed cursor still requires a documented overlap/lookback policy or CDC/change-sequence source contract.
- Replaying a full snapshot during migration is safe only because current Silver notebooks deduplicate repeated entity/event snapshots under their documented key/version rules.
- CI validates local API behavior and exported ADF structure. Applying the SQL migration and publishing the API/ADF assets remain separate deployment operations.
