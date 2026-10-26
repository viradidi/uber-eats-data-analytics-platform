# Silver Transformation Review

## Scope

This review covers the committed Databricks notebook exports in `notebooks/`. It distinguishes code-level behavior from runtime evidence. It does not claim the notebooks were executed against the currently unavailable Azure subscription.

## Current write behavior

| Dataset | Write strategy | Operational implication |
|---|---|---|
| Payments | Delta `overwrite` | Safe only when Bronze input is the complete intended dataset for the target table; an incremental-only input could replace prior Silver rows |
| Deliveries | Delta `overwrite` | Same full-refresh versus incremental-input consideration |
| Order status | Delta `overwrite` | Same full-refresh versus incremental-input consideration |
| Ratings | Delta `overwrite` | Same full-refresh versus incremental-input consideration |
| Drivers | Delta `MERGE` on `driver_id` | Source duplicate-key metrics are now captured before deduplication; prepared merge input is checked for duplicate keys |
| Promotions | Delta `MERGE` on `promo_id` and `restaurant_id` | Source composite-key and discount metrics are now captured before deduplication/filtering; prepared merge input is checked for duplicate composite keys |
| Zones | Delta `MERGE` on `zone_id` | Requires source uniqueness for safe merge behavior; runtime validation remains to be added |

Do not replace overwrite paths with merges until source load patterns and reliable business keys are confirmed. A full-refresh design can legitimately use overwrite; incremental designs generally need merge or another explicitly idempotent strategy.

## Quality controls added

### Drivers

`notebooks/Silver_Drivers.py` reports source row count, null driver IDs, duplicate driver keys before deduplication, missing join dates, unsupported vehicle types, and unsupported statuses. It then preserves the existing transformations and checks that the prepared merge input has no duplicate `driver_id` values.

### Promotions

`notebooks/Silver_Promotions.py` reports source row count, null promotion/restaurant IDs, duplicate composite keys before deduplication, invalid discount values, and missing promotion codes. It preserves the existing deduplication and positive-discount filter, then checks the prepared merge input for duplicate composite keys.

These are notebook logging/guardrails, not a persisted observability system. Metrics currently appear in notebook output; they are not yet written to a durable quality-results table. The current transformations still retain their existing filtering behavior, so metrics do not yet provide a rejected-record quarantine.

## Remaining transformation concerns

### API datasets

The Payments, Deliveries, Order Status, and Ratings notebooks flatten the API `data` array and write to Silver. Many sample sections for type casting, column standardization, null handling, deduplication, and validation remain commented out. Their output validation and source-to-target reconciliation remain open.

### Zones

The Zones notebook merges on `zone_id`, but does not yet measure null keys, duplicate source keys, or coordinate validity before publishing. Its source merge input should be validated before merge.

### Environment portability

Several notebooks append a personal workspace path to `sys.path` and read from a hard-coded ADLS account/container. A portable deployment should supply environment configuration through approved Databricks/Azure configuration and secret mechanisms, rather than depend on another person's workspace path.

### Schema inference

CSV notebooks use `inferSchema=true`. This is convenient for exploration, but inferred types can vary with source files. Repeatable ingestion should use an explicit schema or a deliberate schema-validation step before publishing to Silver.

## Recommended next steps

1. Validate source key uniqueness before every Delta merge, including Zones.
2. Persist quality metrics with a run identifier, source/target counts, and rejected-row counts.
3. Add source-to-target reconciliation for each table.
4. Confirm whether each API Bronze path is a full snapshot or incremental batches before changing write strategies.
5. Add dataset-specific schema/type checks to API notebooks.
6. Move workspace paths and storage account settings into environment configuration.
7. Execute representative runs and reruns in an available Spark/Databricks environment.

## Status

Implemented in repository: pre-cleaning quality metrics and prepared merge-key checks for Drivers and Promotions; CI checks for committed sample CSV fixtures.

Not yet runtime-verified: Databricks notebook execution, Delta writes/merges, source-to-target reconciliation, durable quality metrics, and end-to-end Azure orchestration.
