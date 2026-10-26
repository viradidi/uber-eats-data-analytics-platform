# Silver Transformation Review

## Scope

This review covers the committed Databricks notebook exports in `notebooks/`. It distinguishes code-level behavior from runtime evidence. It does not claim the notebooks were executed against the currently unavailable Azure subscription.

## Current write behavior

| Dataset | Write strategy | Operational implication |
|---|---|---|
| Payments | Delta `overwrite` | Empty flattened API responses now fail before overwrite; non-empty full-refresh semantics and row-level quality rules still need confirmation |
| Deliveries | Delta `overwrite` | Empty flattened API responses now fail before overwrite; non-empty full-refresh semantics and row-level quality rules still need confirmation |
| Order status | Delta `overwrite` | Empty flattened API responses now fail before overwrite; non-empty full-refresh semantics and row-level quality rules still need confirmation |
| Ratings | Delta `overwrite` | Empty flattened API responses now fail before overwrite; non-empty full-refresh semantics and row-level quality rules still need confirmation |
| Drivers | Delta `MERGE` on `driver_id` | Source duplicate-key metrics are now captured before deduplication; prepared merge input is checked for duplicate keys |
| Promotions | Delta `MERGE` on `promo_id` and `restaurant_id` | Source composite-key and discount metrics are now captured before deduplication/filtering; prepared merge input is checked for duplicate composite keys |
| Zones | Delta `MERGE` on `zone_id` | Now measures null/blank keys, duplicate keys, missing city/name, and invalid coordinates; blocks publishing when keys or coordinates are invalid |

Do not replace overwrite paths with merges until source load patterns and reliable business keys are confirmed. A full-refresh design can legitimately use overwrite; incremental designs generally need merge or another explicitly idempotent strategy.

## Quality controls added

### Drivers

`notebooks/Silver_Drivers.py` reports source row count, null driver IDs, duplicate driver keys before deduplication, missing join dates, unsupported vehicle types, and unsupported statuses. It then preserves the existing transformations and checks that the prepared merge input has no duplicate `driver_id` values.

### Promotions

`notebooks/Silver_Promotions.py` reports source row count, null promotion/restaurant IDs, duplicate composite keys before deduplication, invalid discount values, and missing promotion codes. It preserves the existing deduplication and positive-discount filter, then checks the prepared merge input for duplicate composite keys.

### Zones

`notebooks/Silver_Zones.ipynb` reports source row count, null/blank zone IDs, duplicate zone keys, missing city/name fields, and invalid latitude/longitude values. Coordinates are cast to numeric values for validation, so null or nonnumeric values are counted as invalid. The notebook stops before the Delta write/merge if the merge key is missing/duplicated or either coordinate is outside its valid geographic range. The existing `zone_id` merge key and Delta write strategy remain unchanged.

Drivers, Promotions, Zones, Payments, Deliveries, Order Status, and Ratings now append a successful-run record to `ubereats_databricks1.silver.pipeline_quality_results` after the Silver write/merge or overwrite succeeds. Each record includes a generated run ID, dataset name, source row count, prepared row count, target row count after publishing, status, and recorded timestamp. The table is append-only so runs can be compared over time. In addition, all seven Silver publishing paths now catch write/merge exceptions and attempt to append a failure event to `ubereats_databricks1.silver.pipeline_failure_events`, including run ID, dataset, stage, exception type/message, and timestamp; the original exception is re-raised. Failure-event logging covers publish/write failures and API Silver validation and row-count reconciliation failures through `src/common/quality_events.py`. Source-read failures in Drivers, Promotions, Zones, Payments, Deliveries, Order Status, and Ratings are now logged with stage `source_read`, then re-raised. API validation and reconciliation failures use the shared logger too. The helper uses best-effort logging and preserves the original failure if event persistence itself fails. If the failure log itself cannot be written, the notebook prints a logging error and preserves the original exception. Rejected-row quarantine now writes invalid required-field rows from all four API Silver datasets, invalid payment amounts and ratings, and every row belonging to duplicate business-key groups to `ubereats_databricks1.silver.pipeline_rejected_records`. Each record stores a run ID, dataset, rejection reason, timestamp, and JSON row payload. The validation still fails after quarantine, preventing invalid input from being published. Drivers now quarantine null `driver_id` rows and unsupported vehicle-type rows before their existing filters. Promotions quarantine rows with missing merge keys and invalid discounts; missing merge keys stop publishing after quarantine to avoid unsafe Delta merges. Zones quarantine rows with missing/blank IDs or invalid coordinates before the existing fail-fast checks. Duplicate-key groups are now quarantined in Drivers, Promotions, and Zones; because no deterministic winner policy is defined, each pipeline fails the batch when duplicate business-key groups are present rather than arbitrarily choosing a row. API full-refresh tables retain their prepared-versus-published row-count reconciliation. A reusable `reconcile_merge_target()` helper now checks prepared source-key coverage and null-safe equality for selected fields after the Drivers, Promotions, and Zones Delta merges. It appends outcomes to `ubereats_databricks1.silver.pipeline_reconciliation_results`; mismatches are logged as `silver_reconciliation` failures and raise before the run is marked successful. The checks cover Drivers (`driver_id`, `vehicle_type`, `status`, `city`, `join_date`), Promotions (`promo_id`, `restaurant_id`, `discount_value`, `promo_code`), and Zones (`zone_id`, `latitude`, `longitude`, `city`, `zone_name`). A separate `reconcile_full_snapshot()` utility now supports explicit full-snapshot audits: it checks source-only and target-only keys, total row count, and optional configured aggregates, and writes detailed checks to `ubereats_databricks1.silver.pipeline_reconciliation_details`. It never deletes target rows. The caller must pass `source_is_full_snapshot=True`; do not use it for incremental batches. Drivers, Promotions, and Zones have not been wired to call this utility because the source completeness semantics have not been confirmed. The shared failure logger accepts an optional existing run ID, allowing reconciliation detail records and corresponding failure events to be correlated. Its own write errors are intentionally swallowed and reported to stdout so they do not mask the original pipeline failure. Unit tests cover run-ID propagation and logging-write failure isolation; runtime execution remains pending.

## Remaining transformation concerns

### API datasets

The Payments, Deliveries, Order Status, and Ratings notebooks flatten the API `data` array, report the flattened row count, and fail before the existing Delta overwrite if the payload is empty. After a successful overwrite, they compare the prepared row count with the published target row count, then append a durable quality record containing run ID, source/prepared/target row counts, reconciliation status, and timestamp to `ubereats_databricks1.silver.pipeline_quality_results`. A mismatch is recorded as `FAILED_RECONCILIATION` and fails the notebook. This is row-count reconciliation for the four full-refresh API tables; it does not yet validate field-level values or target-only historical keys. Many sample sections for type casting, column standardization, null handling, deduplication, and validation remain commented out. Non-empty payload schema/business validation now has fail-fast gates for required columns, null required fields, and business-key uniqueness in both notebook exports and notebooks. Payments also validates nonnegative numeric amounts; Ratings validates numeric ratings in the inclusive 1–5 range. The Order Status business key is `(order_id, status_timestamp)` so multiple status events per order remain valid. These checks fail the run rather than silently dropping invalid records. Field-level reconciliation for merge-based tables is implemented in code. Rejected-row quarantine now covers the specified invalid Drivers, Promotions, and Zones rows. An opt-in full-snapshot reconciliation utility is implemented, but pipeline integration remains open until source snapshot semantics are confirmed.

### Zones

The Zones notebook now validates key uniqueness and coordinate ranges before publishing. Missing city or zone-name values are reported but are not currently fatal; decide whether to quarantine or reject those records as an explicit business rule.

### Environment portability

The Silver notebook exports no longer append a personal workspace path. They import `standardize_columns` from the repository-owned `src/common/spark_utils.py` module, which normalizes column names to lowercase snake_case and raises if normalization would create duplicate names.

Each Silver notebook now defines a Databricks widget named `bronze_root`. The default points to the project's original Bronze ADLS location for backward compatibility; set the widget to the destination environment's root when running the notebook or job. Dataset paths are constructed relative to that root (for example, `<bronze_root>/csv/drivers/` and `<bronze_root>/api/payments/`). The root is a location, not a credential. Configure storage authentication separately using managed identity, service-principal configuration, or the approved credential mechanism for the workspace. No storage key or secret should be placed in the widget.

The code-level portability change is committed, but the notebooks have not been run against a live Spark/Databricks workspace after this change. Confirm the repository root is available on the Python path when importing the shared `src` package; if notebooks are deployed outside a Databricks Git folder, package the shared module or configure the workspace path accordingly.

### Schema inference

CSV notebooks use `inferSchema=true`. This is convenient for exploration, but inferred types can vary with source files. Repeatable ingestion should use an explicit schema or a deliberate schema-validation step before publishing to Silver.

## Recommended next steps

1. Extend equivalent source-key and required-field checks to the API-backed Silver notebooks.
2. Extend failure-event logging to source-read and pre-publish validation failures, then add rejected-row quarantine with run identifiers.
3. Decide and document whether each Drivers, Promotions, and Zones source is a complete snapshot or an incremental extract. Only after that decision, wire `reconcile_full_snapshot()` into confirmed full-snapshot runs and configure dataset-appropriate aggregates. The utility reports target-only keys without deleting them.
4. Confirm whether each API Bronze path is a full snapshot or incremental batches before changing write strategies.
5. Add dataset-specific schema/type checks to API notebooks.
6. Run the portable notebook exports in a Databricks Git folder, set `bronze_root` for the target environment, and validate storage authentication.
7. Execute representative runs and reruns in an available Spark/Databricks environment.

## Status

Implemented in repository: API Silver fail-fast required-column, required-field, business-key uniqueness, payment amount, and rating range checks in both Python exports and notebooks; reusable best-effort failure event logging for API Silver source-read, validation, reconciliation, and publishing errors via `src/common/quality_events.py`; durable JSON quarantine for required-field errors and selected domain-rule errors;    pre-cleaning quality metrics and prepared merge-key checks for Drivers and Promotions; Drivers quarantines null IDs and unsupported vehicle types; Promotions quarantines missing merge keys and invalid discounts and blocks publishing on null keys; Zones quarantines invalid IDs/coordinates and fails before publishing on those conditions; Drivers, Promotions, Zones, Payments, Deliveries, Order Status, and Ratings append successful-run metrics to `ubereats_databricks1.silver.pipeline_quality_results` and attempt to log publishing failures in `ubereats_databricks1.silver.pipeline_failure_events`; Payments, Deliveries, Order Status, and Ratings fail before overwrite on empty flattened API payloads and compare prepared versus published target row counts; CI checks committed sample CSV fixtures.

Not yet runtime-verified: Databricks notebook execution, Delta writes/merges, source-to-target reconciliation, durable quality metrics, and end-to-end Azure orchestration.
