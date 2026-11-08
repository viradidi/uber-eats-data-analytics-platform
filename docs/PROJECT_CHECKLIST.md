# Project Completion and Evidence Checklist

Use this checklist to track delivered artifacts and validation. **Current status: repository implementation, Azure deployment, and end-to-end runtime acceptance are complete according to the project owner.** The owner confirms that the deployed API, CSV, and SQL ingestion paths, Bronze/Silver processing, incremental replay, Gold publication, and reconciliation were exercised successfully. The latest repository CI passed 135 tests in [GitHub Actions run 41984330196](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/42021275338) on commit `034a19d5d62a94bb75e510f03e214f59590bf54a`. Exact Azure run IDs, screenshots, logs, and measured row counts have not yet been added to the repository; see [the deployment acceptance record](DEPLOYMENT_ACCEPTANCE_RECORD.md). That remaining evidence curation does not mean deployment or execution is pending.

## Repository and documentation

- [x] Project README created
- [x] Logical architecture documented
- [x] Verify all README links
- [x] Canonical architecture diagram committed as an SVG; an additional screenshot is optional and not a release requirement
- [x] Review reused project assets: no third-party media or copied external source code is identified in the delivered artifacts; runtime packages are declared as dependencies rather than vendored.

## Ingestion

- [x] Verify API endpoints and authentication
- [x] Validate API ingestion output
- [x] Validate CSV ingestion output
- [x] Review SQL incremental extraction boundaries and checkpoint dependencies in the exported ADF definition
- [x] Document null-watermark, equal-boundary/late-update, dynamic SQL metadata, and replay/idempotency limitations in [the SQL incremental review](SQL_INCREMENTAL_LOAD_REVIEW.md); the configured end-to-end SQL path is owner-confirmed as deployed and validated, while the documented source-change limitations remain in force
- [x] Add [read-only SQL control-table preflight queries](SQL_INCREMENTAL_CONTROL_PREFLIGHT.sql) for missing metadata, invalid simple identifiers, duplicate control IDs/mappings, and configured source schema/table/column existence with watermark data type visibility; add static CI contract coverage for the new ADF guards that reject null/duplicate control IDs and duplicate active source mappings before the source loop. Reapply the approved allowlist review and execute the new guards after redeployment.
- [x] Add [incremental-run acceptance query templates](SQL_INCREMENTAL_RUN_ACCEPTANCE_QUERIES.md) for bounded source counts, landed-batch duplicate keys, count reconciliation, and checkpoint-advancement evidence; complete and retain source-specific adaptations and outputs during the next environment acceptance cycle
- [x] Set pipeline-level concurrency to `1` and add a transactional prior-watermark compare-and-swap update with rollback and SQL conflict error; the configured path was included in the owner-confirmed Azure acceptance. External or cross-pipeline writer protection remains a design boundary
- [x] Derive SQL incremental landing filenames from the control ID and lower/upper watermark range for deterministic batch identity, with `.csv` sink extension alignment
- [x] Verify ADLS overwrite behavior and run the repository-owned SQL Silver Delta MERGE twice on the same captured interval in the deployed environment (end-to-end replay validation owner-confirmed; detailed comparison export not yet attached)
- [x] Add a per-source [incremental extraction contract](INCREMENTAL_SOURCE_CONTRACT.md) covering initial load, keys, watermark semantics, deletes, replay, reconciliation, and runtime acceptance tests
- [x] Test reruns and partial-failure recovery

## Transformation and quality

- [x] Validate notebook inputs and outputs in a Spark environment
- [x] Make Bronze storage root configurable through a Databricks widget in all Silver notebooks
- [x] Document transformation behavior and reliability findings in `docs/SILVER_TRANSFORMATION_REVIEW.md`
- [x] Add CI tests for required columns, nonblank keys, key uniqueness, allowed CSV domain values, promotion dates/discounts, and zone coordinate ranges in committed sample fixtures
- [x] Add pre-cleaning quality metrics and prepared merge-key checks to Drivers and Promotions Silver notebooks
- [x] Prevent empty flattened API payloads from overwriting existing Payments, Deliveries, Order Status, and Ratings Silver tables
- [x] Add schema, required-field, business-key uniqueness, and dataset-specific value checks to the API Silver notebook exports (runtime execution validated by project owner)
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
- [x] Confirm source completeness semantics and wire full-snapshot reconciliation only into merge pipelines proven to consume complete snapshots
- [x] Persist successful-run quality metrics for Payments, Deliveries, Order Status, and Ratings after successful Delta overwrite
- [x] Reconcile prepared and published target row counts for API-backed full-refresh Silver tables
- [x] Add source-key coverage and null-safe field-level reconciliation after Drivers, Promotions, and Zones Delta merges, with durable reconciliation outcomes and fail-fast mismatch handling (runtime execution validated by project owner)
- [x] Implement opt-in target-only stale-key detection and configurable aggregate comparisons in a separate reconciliation detail table
- [x] Correlate reconciliation failure events with their pipeline run IDs and test best-effort logging behavior
- [x] Configure and execute dataset-specific aggregate checks in Databricks after source snapshot semantics are confirmed

## Security

- [x] Review linked-service JSON for secrets
- [x] Confirm no private credentials are tracked
- [x] Confirm sample data is appropriate for public release
- [x] Document configurable Bronze storage root for Silver notebooks without embedding credentials
- [x] Remove personal workspace path dependency and add repository-owned shared Spark utilities

## Operations

- [x] Record owner-confirmed historical successful execution; the original run IDs and detailed logs were not retained and are explicitly documented as such.
- [x] Add [read-only Databricks SQL audit queries](RECONCILIATION_AUDIT_QUERIES.sql) for latest quality outcomes, reconciliation failures, failure-event correlation, rejected-row summaries, and merge-reconciliation outcomes. The SQL is code-reviewed for table-reference consistency; live execution validated by project owner.
- [x] Add a [dataset-level operational health report](OPERATIONAL_HEALTH_REPORT.sql) combining latest quality and merge-reconciliation outcomes with failure and quarantine volumes from the last 24 hours. Statuses remain NULL when no recorded result exists; the report is read-only and validated by project owner against the target environment.
- [x] Add a [recovery and rerun runbook](RECOVERY_AND_RERUN_RUNBOOK.md) covering failure triage, SQL incremental recovery, Silver notebook recovery, checkpoint advancement gates, and rerun acceptance checks. Procedures are documented; live Azure recovery validated by project owner.
- [x] Add a [data-quality trends and incident analysis report](DATA_QUALITY_TRENDS_AND_INCIDENTS.sql) showing daily quality outcomes, reconciliation misses/mismatches, failure-event counts, rejected-row volume, and day-over-day changes. It reports event counts rather than claiming incident duration; live Databricks execution validated by project owner.
- [x] Document failure handling and recovery
- [x] Document deployment prerequisites
- [x] Record known limitations
- [x] Add CI checks for repository structure, Python syntax, API key configuration, and sample CSV quality (successful run confirmed for the current workflow revision)

## Release

A portfolio-website listing is optional external publishing work and is not a project-completion or Azure-release gate.

- [x] Verify the README renders correctly on GitHub
- [x] Align the secondary architecture overview with the canonical SVG and detailed architecture document
- [x] Align project status language across README, architecture, portfolio evidence, and final handoff


## Merge-based Silver reconciliation milestone

The Drivers, Promotions, and Zones merge-based Silver pipelines call `reconcile_merge_target()` after publishing. The helper checks source-key coverage and selected fields with null-safe equality and records outcomes in `ubereats_databricks1.silver.pipeline_reconciliation_results`. The separate `reconcile_full_snapshot()` utility checks source-only and target-only keys, row counts, and optional aggregates, writing to `ubereats_databricks1.silver.pipeline_reconciliation_details`. It requires explicit `source_is_full_snapshot=True` and never deletes target rows. It is intentionally not called by the three notebooks because their source completeness semantics have not been established as full snapshots; the separate merge reconciliation path is used for those pipelines. All Spark/Delta runtime execution validated by project owner.

## API Silver validation milestone

The Payments, Deliveries, Order Status, and Ratings notebook exports fail before Delta overwrite when required columns are missing, required fields are null, or configured business keys are duplicated. Rejected rows for null required fields and duplicate-key groups are appended to `ubereats_databricks1.silver.pipeline_rejected_records` before validation fails. Payments additionally quarantines null, nonnumeric, or negative amounts; Ratings quarantines null, nonnumeric, or out-of-range ratings (valid range 1–5). The Order Status key is the composite `(order_id, status_timestamp)` because an order can legitimately have multiple status events. Validation and quarantine are implemented in both `.py` exports and `.ipynb` notebooks; live Spark execution validated by project owner.


## Merge-based rejected-record quarantine milestone

Drivers now appends rows with null `driver_id` and unsupported `vehicle_type` to `ubereats_databricks1.silver.pipeline_rejected_records` before the existing filters run. Promotions quarantines null `promo_id`/`restaurant_id` rows and stops before publishing when either merge key is null; null or nonpositive discounts are quarantined before the existing positive-discount filter. Zones quarantines rows with missing/blank `zone_id` values or invalid coordinates before its existing fail-fast checks. Quarantine records carry the shared helper's run ID, dataset name, rejection reason, timestamp, and JSON payload. Historical milestone note: duplicate-key quarantine was added in a later milestone below. The current policy is to quarantine all rows in each duplicate-key group and fail the batch before merge; live Delta/Spark execution validated by project owner.


## Merge-based duplicate-key handling milestone

Drivers, Promotions, and Zones now identify duplicate business-key groups, persist every row in those groups to `ubereats_databricks1.silver.pipeline_rejected_records`, and fail the batch before the Delta merge. No winner is chosen nondeterministically. Drivers use `driver_id`, Promotions use `(promo_id, restaurant_id)`, and Zones use `zone_id`. This is a deliberate fail-fast policy until a source-specific winner rule (for example, latest source update timestamp with a stable tie-breaker) is established. Live Spark/Delta execution validated by project owner.
- [x] Add static recovery contract tests in `tests/test_recovery_contract.py` for ADF copy/job/watermark dependency gates, checkpoint safety language, operational report links, and explicit documentation of watermark edge cases. These tests run in the existing standard-library unittest CI step; they do not execute Azure services.
- [x] Document the boundary between repository-level recovery contract tests and live Azure validation in the recovery runbook.


## Consolidated release-readiness view

See the [consolidated release-readiness review](RELEASE_READINESS_REVIEW.md), [deployment acceptance record](DEPLOYMENT_ACCEPTANCE_RECORD.md), and [Azure deployment runbook](RESTORED_AZURE_ACCEPTANCE_PLAN.md) for the accepted runtime status, evidence boundary, and repeatable procedure.

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

### Evidence retention and optional follow-up
- [x] Execute ADF pipeline validation in the deployed environment; end-to-end success is owner-confirmed. Record controlled failure/recovery scenarios separately and only when those scenarios were actually exercised
- [x] Execute the configured Silver/Gold Spark/Delta path in the deployed Databricks environment; end-to-end success is owner-confirmed. Exact notebook run exports are not yet attached
- [x] Complete and approve source-specific contracts for the enabled SQL source configuration; the successful tested interval is owner-confirmed. This does not imply universal late-arrival or hard-delete capture beyond each documented source contract
- [x] Replace deleted-workspace SQL Silver job `121055824417886` with repository-owned `Silver_SQL_Incremental`; ADF now passes one exact bounded CSV batch and approved metadata to a version-aware Delta MERGE notebook
- [x] Add disabled-by-default `dbo.sql_silver_control` migration, pre-copy ADF metadata gate, workspace import, key reconciliation, run audit, and local tests
- [x] Configure and approve the active SQL source target, business key, required fields, Spark type map, cursor semantics, and delete treatment; same-interval replay was part of the owner-confirmed deployed acceptance
- [x] Record that detailed historical run IDs/logs were not retained and establish the runtime evidence register for the completed deployment; attach sanitized current run exports when collected
- [x] Complete final README link/render review (portfolio-site listing remains optional)

A checked repository artifact means the relevant code or documentation is present. Runtime validation items marked complete reflect the project owner's confirmation that validation was performed; retain available run logs and outputs as supporting review evidence.


## Gold analytics extension — deployed and end-to-end validated

- [x] Add shared PySpark transformations for order-level, daily order, payment-method, restaurant-feedback, and driver-performance outputs.
- [x] Add Gold Databricks notebook exports and an ADF master pipeline that waits for API, CSV, and SQL ingestion success before Gold publication.
- [x] Synchronize all six paired Databricks SOURCE `.py` and Jupyter `.ipynb` exports cell-for-cell; keep `.py` canonical for workspace deployment and enforce parity in CI.
- [x] Separate SQL source catalog validation from control-database checkpoint updates in ADF; add a regression contract for independent source/control SQL endpoints.
- [x] Add a read-only source SQL catalog inventory for cursor metadata and primary/unique-index key candidates; optional table scans are disabled by default and candidates still require source-owner approval.
- [x] Add business reporting SQL with documented grains and metric definitions.
- [x] Add local Spark tests and static ADF dependency-contract tests to CI.
- [x] Add Gold run-audit design and run-evidence retention guidance.
- [x] Fix generated API test payments to populate both payment_timestamp and updated_at as required by the Payments Silver contract.
- [x] Run the local PySpark tests in GitHub Actions and review the actual workflow result: [135 tests passed in run 41984330196](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/42021275338).
- [x] Import Gold_Analytics into the target Databricks workspace and make src.gold available to the notebook (deployed end-to-end acceptance owner-confirmed).
- [x] Publish and run the master ADF pipeline in the deployed environment (owner-confirmed).
- [x] Verify all five Gold tables, output counts, gold_run_audit rows, and reconciliation results (owner-confirmed; exact counts and exports not yet attached).
- [ ] Attach actual ADF and Databricks run IDs, URLs, timestamps, and outputs using docs/run-evidence/runtime-evidence-template.md (evidence curation only; deployment and end-to-end operation are complete).

The existing architecture SVG remains unchanged; the Gold extension is documented in the architecture text and the Gold layer guide. The repository contains reporting SQL, not a deployed Power BI dashboard.
