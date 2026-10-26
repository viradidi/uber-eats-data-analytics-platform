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

Drivers, Promotions, Zones, Payments, Deliveries, Order Status, and Ratings now append a successful-run record to `ubereats_databricks1.silver.pipeline_quality_results` after the Silver write/merge or overwrite succeeds. Each record includes a generated run ID, dataset name, source row count, prepared row count, target row count after publishing, status, and recorded timestamp. The table is append-only so runs can be compared over time. This first increment records successful runs only; failed-run records, rejected-row quarantine, and row-level reconciliation remain open.

## Remaining transformation concerns

### API datasets

The Payments, Deliveries, Order Status, and Ratings notebooks flatten the API `data` array, report the flattened row count, and fail before the existing Delta overwrite if the payload is empty. After a successful overwrite, they append a durable quality record containing run ID, source/prepared/target row counts, status, and timestamp to `ubereats_databricks1.silver.pipeline_quality_results`. This prevents a transient empty API response from silently replacing a previously populated Silver table and makes successful full-refresh runs visible for monitoring. Many sample sections for type casting, column standardization, null handling, deduplication, and validation remain commented out. Non-empty payload schema/business validation and source-to-target reconciliation remain open.

### Zones

The Zones notebook now validates key uniqueness and coordinate ranges before publishing. Missing city or zone-name values are reported but are not currently fatal; decide whether to quarantine or reject those records as an explicit business rule.

### Environment portability

Several notebooks append a personal workspace path to `sys.path` and read from a hard-coded ADLS account/container. A portable deployment should supply environment configuration through approved Databricks/Azure configuration and secret mechanisms, rather than depend on another person's workspace path.

### Schema inference

CSV notebooks use `inferSchema=true`. This is convenient for exploration, but inferred types can vary with source files. Repeatable ingestion should use an explicit schema or a deliberate schema-validation step before publishing to Silver.

## Recommended next steps

1. Extend equivalent source-key and required-field checks to the API-backed Silver notebooks.
2. Add failed-run records for all Silver datasets and preserve run identifiers across failure reporting.
3. Add source-to-target reconciliation for each table.
4. Confirm whether each API Bronze path is a full snapshot or incremental batches before changing write strategies.
5. Add dataset-specific schema/type checks to API notebooks.
6. Move workspace paths and storage account settings into environment configuration.
7. Execute representative runs and reruns in an available Spark/Databricks environment.

## Status

Implemented in repository: pre-cleaning quality metrics and prepared merge-key checks for Drivers and Promotions; Zones reports key/coordinate quality and fails before publishing on invalid keys or coordinates; Drivers, Promotions, Zones, Payments, Deliveries, Order Status, and Ratings append successful-run metrics to `ubereats_databricks1.silver.pipeline_quality_results`; Payments, Deliveries, Order Status, and Ratings fail before overwrite on empty flattened API payloads; CI checks committed sample CSV fixtures.

Not yet runtime-verified: Databricks notebook execution, Delta writes/merges, source-to-target reconciliation, durable quality metrics, and end-to-end Azure orchestration.
