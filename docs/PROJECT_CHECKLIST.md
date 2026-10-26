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
- [x] Document null-watermark, equal-boundary/late-update, dynamic SQL metadata, and replay/idempotency limitations in [the SQL incremental review](SQL_INCREMENTAL_LOAD_REVIEW.md); remediation and live validation remain pending
- [x] Add a per-source [incremental extraction contract](INCREMENTAL_SOURCE_CONTRACT.md) covering initial load, keys, watermark semantics, deletes, replay, reconciliation, and runtime acceptance tests
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
- [x] Extend Silver failure logging to source-read failures for Drivers, Promotions, and Zones, and retain API Silver validation, reconciliation, and publish failure logging
- [x] Add JSON-based rejected-row quarantine for API Silver required-field failures and Payments/Ratings domain-rule failures, with run IDs and rejection reasons
- [x] Quarantine duplicate-key groups for Payments, Deliveries, Order Status, and Ratings before failing validation
- [x] Quarantine null driver IDs and unsupported vehicle types for Drivers; quarantine missing merge keys and invalid discounts for Promotions; quarantine invalid zone IDs/coordinates for Zones
- [x] Quarantine all rows in duplicate-key groups for Drivers, Promotions, and Zones; fail the batch because no deterministic winner policy is defined
- [x] Add a reusable non-destructive full-snapshot reconciliation utility that requires explicit source_is_full_snapshot=True and records target-only/source-only keys, row counts, and optional aggregate comparisons
- [ ] Confirm source completeness semantics and wire full-snapshot reconciliation into only those merge pipelines proven to consume complete snapshots
- [x] Persist successful-run quality metrics for Payments, Deliveries, Order Status, and Ratings after successful Delta overwrite
- [x] Reconcile prepared and published target row counts for API-backed full-refresh Silver tables
- [x] Add source-key coverage and null-safe field-level reconciliation after Drivers, Promotions, and Zones Delta merges, with durable reconciliation outcomes and fail-fast mismatch handling (runtime execution pending)
- [x] Implement opt-in target-only stale-key detection and configurable aggregate comparisons in a separate reconciliation detail table
- [x] Correlate reconciliation failure events with their pipeline run IDs and test best-effort logging behavior
- [ ] Configure and execute dataset-specific aggregate checks in Databricks after source snapshot semantics are confirmed

## Security

- [ ] Review linked-service JSON for secrets
- [ ] Confirm no private credentials are tracked
- [ ] Confirm sample data is appropriate for public release
- [x] Document configurable Bronze storage root for Silver notebooks without embedding credentials
- [x] Remove personal workspace path dependency and add repository-owned shared Spark utilities

## Operations

- [ ] Capture successful pipeline execution evidence
- [x] Add [read-only Databricks SQL audit queries](RECONCILIATION_AUDIT_QUERIES.sql) for latest quality outcomes, reconciliation failures, failure-event correlation, rejected-row summaries, and merge-reconciliation outcomes. The SQL is code-reviewed for table-reference consistency; live execution is pending an available Databricks runtime.
- [x] Add a [dataset-level operational health report](OPERATIONAL_HEALTH_REPORT.sql) combining latest quality and merge-reconciliation outcomes with failure and quarantine volumes from the last 24 hours. Statuses remain NULL when no recorded result exists; the report is read-only and has not been live-executed against Databricks.
- [x] Add a [recovery and rerun runbook](RECOVERY_AND_RERUN_RUNBOOK.md) covering failure triage, SQL incremental recovery, Silver notebook recovery, checkpoint advancement gates, and rerun acceptance checks. Procedures are documented; live Azure recovery remains unverified.
- [x] Add a [data-quality trends and incident analysis report](DATA_QUALITY_TRENDS_AND_INCIDENTS.sql) showing daily quality outcomes, reconciliation misses/mismatches, failure-event counts, rejected-row volume, and day-over-day changes. It reports event counts rather than claiming incident duration; live Databricks execution remains pending.
- [ ] Document failure handling and recovery
- [ ] Document deployment prerequisites
- [ ] Record known limitations
- [x] Add CI checks for repository structure, Python syntax, API key configuration, and sample CSV quality (successful run confirmed for the current workflow revision)

## Release

- [ ] Verify the README renders correctly on GitHub
- [ ] Add the repository to the portfolio website


## Merge-based Silver reconciliation milestone

The Drivers, Promotions, and Zones merge-based Silver pipelines call `reconcile_merge_target()` after publishing. The helper checks source-key coverage and selected fields with null-safe equality and records outcomes in `ubereats_databricks1.silver.pipeline_reconciliation_results`. The separate `reconcile_full_snapshot()` utility checks source-only and target-only keys, row counts, and optional aggregates, writing to `ubereats_databricks1.silver.pipeline_reconciliation_details`. It requires explicit `source_is_full_snapshot=True` and never deletes target rows. It is not yet called by the three notebooks because their source completeness semantics must be confirmed first. All Spark/Delta runtime execution remains pending.

## API Silver validation milestone

The Payments, Deliveries, Order Status, and Ratings notebook exports fail before Delta overwrite when required columns are missing, required fields are null, or configured business keys are duplicated. Rejected rows for null required fields and duplicate-key groups are appended to `ubereats_databricks1.silver.pipeline_rejected_records` before validation fails. Payments additionally quarantines null, nonnumeric, or negative amounts; Ratings quarantines null, nonnumeric, or out-of-range ratings (valid range 1–5). The Order Status key is the composite `(order_id, status_timestamp)` because an order can legitimately have multiple status events. Validation and quarantine are implemented in both `.py` exports and `.ipynb` notebooks; live Spark execution remains pending.


## Merge-based rejected-record quarantine milestone

Drivers now appends rows with null `driver_id` and unsupported `vehicle_type` to `ubereats_databricks1.silver.pipeline_rejected_records` before the existing filters run. Promotions quarantines null `promo_id`/`restaurant_id` rows and stops before publishing when either merge key is null; null or nonpositive discounts are quarantined before the existing positive-discount filter. Zones quarantines rows with missing/blank `zone_id` values or invalid coordinates before its existing fail-fast checks. Quarantine records carry the shared helper's run ID, dataset name, rejection reason, timestamp, and JSON payload. Historical milestone note: duplicate-key quarantine was added in a later milestone below. The current policy is to quarantine all rows in each duplicate-key group and fail the batch before merge; live Delta/Spark execution remains pending.


## Merge-based duplicate-key handling milestone

Drivers, Promotions, and Zones now identify duplicate business-key groups, persist every row in those groups to `ubereats_databricks1.silver.pipeline_rejected_records`, and fail the batch before the Delta merge. No winner is chosen nondeterministically. Drivers use `driver_id`, Promotions use `(promo_id, restaurant_id)`, and Zones use `zone_id`. This is a deliberate fail-fast policy until a source-specific winner rule (for example, latest source update timestamp with a stable tie-breaker) is established. Live Spark/Delta execution remains pending.
- [x] Add static recovery contract tests in `tests/test_recovery_contract.py` for ADF copy/job/watermark dependency gates, checkpoint safety language, operational report links, and explicit documentation of watermark edge cases. These tests run in the existing standard-library unittest CI step; they do not execute Azure services.
- [x] Document the boundary between repository-level recovery contract tests and live Azure validation in the recovery runbook.


## Consolidated release-readiness view

### Repository artifacts available
- [x] SQL incremental pipeline reviewed for dependency ordering and watermark boundary limitations
- [x] Per-source incremental contract and recovery runbook documented
- [x] Standard-library repository validation workflow and static recovery contract tests committed
- [x] README local-link and roadmap consistency checks added to the standard-library test suite
- [x] Mock API timestamp, pagination, incremental-filter, and API-key behavior tests added; CI installs the declared API dependencies before running tests
- [x] Mock payment generator uses the highest existing numeric payment-ID suffix and atomic JSON replacement; tests cover replacement failure preserving the original file
- [x] Mock API source fixtures resolve relative to the application module rather than the server process's working directory; path-resolution tests added
- [x] README documents the local repository test command and clarifies that the file-backed test-data generator is demo-only, not a concurrent production write service
- [x] Operational health, trend, and reconciliation SQL reports committed
- [x] Silver validation, quarantine, quality-event, and reconciliation implementation artifacts committed

### Still requiring environment-specific evidence
- [ ] Execute ADF pipeline validation and controlled recovery scenarios in an available Azure environment
- [ ] Execute Spark/Delta notebooks and audit SQL against a compatible Databricks environment
- [ ] Complete source-specific incremental contracts before claiming reliable change capture for each source
- [ ] Capture successful-run, failure, retry, and reconciliation evidence
- [ ] Complete final README link/render review and portfolio-site project listing

A checked repository artifact means the relevant code or documentation is present. It does not imply live Azure/Databricks execution unless the checklist explicitly says so.
