# Project Completion and Evidence Checklist

Use this checklist to distinguish available artifacts from verified implementation.

## Repository and documentation

- [x] Project README created
- [x] Logical architecture documented
- [ ] Verify all README links
- [ ] Add an approved architecture screenshot if useful
- [ ] Record the source and licensing of any reused material

## Ingestion

- [ ] Verify API endpoints and authentication
- [ ] Validate API ingestion output
- [ ] Validate CSV ingestion output
- [x] Review SQL incremental extraction boundaries and checkpoint dependencies in the exported ADF definition
- [ ] Test reruns and partial-failure recovery

## Transformation and quality

- [ ] Validate notebook inputs and outputs in a Spark environment
- [x] Make Bronze storage root configurable through a Databricks widget in all Silver notebooks
- [x] Document transformation behavior and reliability findings in `docs/SILVER_TRANSFORMATION_REVIEW.md`
- [x] Add CI tests for required columns, nonblank keys, key uniqueness, allowed CSV domain values, promotion dates/discounts, and zone coordinate ranges in committed sample fixtures
- [x] Add pre-cleaning quality metrics and prepared merge-key checks to Drivers and Promotions Silver notebooks
- [x] Prevent empty flattened API payloads from overwriting existing Payments, Deliveries, Order Status, and Ratings Silver tables
- [x] Add schema, required-field, business-key uniqueness, and dataset-specific value checks to the API Silver notebook exports (runtime execution still pending)
- [x] Add null-key and duplicate-key checks to the Zones merge input
- [x] Add latitude/longitude validity checks and fail-fast guardrails to Zones
- [x] Persist successful-run quality metrics with run ID, dataset, source/prepared/target counts, status, and timestamp for Drivers, Promotions, and Zones
- [x] Log Silver write/merge failures to `pipeline_failure_events` with run ID, dataset, exception type/message, stage, and timestamp
- [ ] Extend failure logging to source-read and pre-publish validation errors
- [ ] Add rejected-row details with a run identifier
- [x] Persist successful-run quality metrics for Payments, Deliveries, Order Status, and Ratings after successful Delta overwrite
- [x] Reconcile prepared and published target row counts for API-backed full-refresh Silver tables
- [ ] Add field-level reconciliation and source-to-target reconciliation for merge-based Silver tables

## Security

- [ ] Review linked-service JSON for secrets
- [ ] Confirm no private credentials are tracked
- [ ] Confirm sample data is appropriate for public release
- [x] Document configurable Bronze storage root for Silver notebooks without embedding credentials
- [x] Remove personal workspace path dependency and add repository-owned shared Spark utilities

## Operations

- [ ] Capture successful pipeline execution evidence
- [ ] Document failure handling and recovery
- [ ] Document deployment prerequisites
- [ ] Record known limitations
- [x] Add CI checks for repository structure, Python syntax, API key configuration, and sample CSV quality (successful run confirmed for the current workflow revision)

## Release

- [ ] Verify the README renders correctly on GitHub
- [ ] Add the repository to the portfolio website


## API Silver validation milestone

The Payments, Deliveries, Order Status, and Ratings notebook exports now fail before Delta overwrite when required columns are missing, required fields are null, or configured business keys are duplicated. Payments additionally rejects null, nonnumeric, or negative amounts; Ratings rejects null, nonnumeric, or out-of-range ratings (valid range 1–5). The Order Status key is the composite `(order_id, status_timestamp)` because an order can legitimately have multiple status events. These are fail-fast gates, not a quarantine flow: invalid rows are not silently filtered or discarded. Validation is implemented in both `.py` exports and `.ipynb` notebooks; live Spark execution remains pending.
