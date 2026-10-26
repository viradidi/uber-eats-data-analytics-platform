# Silver Transformation Review

## Scope

This review covers the committed Databricks notebook exports in `notebooks/`. It is a repository artifact review; it does not claim the notebooks were executed against the currently disabled Azure subscription.

## Findings

### 1. Write behavior differs by dataset

| Dataset | Current write strategy in exported notebook | Operational implication |
|---|---|---|
| Payments | Delta `overwrite` | Safe only when the Bronze input represents the complete intended dataset for the target table; an incremental-only input could replace prior Silver rows |
| Deliveries | Delta `overwrite` | Same full-refresh versus incremental-input consideration |
| Order status | Delta `overwrite` | Same full-refresh versus incremental-input consideration |
| Ratings | Delta `overwrite` | Same full-refresh versus incremental-input consideration |
| Drivers | Delta `MERGE` on `driver_id` | Supports repeatable inserts/updates when the key is unique in the source batch |
| Promotions | Delta `MERGE` on `promo_id` and `restaurant_id` | Supports repeatable inserts/updates when the composite key is unique in the source batch |
| Zones | Delta `MERGE` on `zone_id` | Supports repeatable inserts/updates when the key is unique in the source batch |

Do not replace the overwrite paths with merges until the source's intended load pattern and reliable business keys are confirmed. A full-refresh design can legitimately use overwrite; an incremental design generally needs merge or another explicitly idempotent strategy.

### 2. Standardization and validation are uneven

The API notebooks flatten the `data` array and write the result to Silver, while many sample sections for type casting, column standardization, null handling, deduplication, and validation remain commented out. The Drivers notebook applies some standardization and filtering, but its duplicate check occurs after `drop_duplicates`, so it reports duplicates remaining after deduplication rather than measuring how many duplicate keys were present in the original input.

### 3. Environment-specific imports and storage paths

Several notebooks append a personal workspace path to `sys.path` and read from a hard-coded ADLS account/container. These are deployment-specific settings. A portable deployment should supply environment configuration through approved Databricks/Azure configuration and secret mechanisms, not depend on another person's workspace path.

### 4. Schema inference

The CSV notebooks use `inferSchema=true`. This is convenient for exploration, but inferred types can vary with source files. For repeatable production ingestion, use an explicit schema or a deliberate schema-validation step before publishing to Silver.

## Recommended implementation sequence

1. Confirm for each source whether Bronze holds a complete snapshot or incremental batches.
2. Confirm stable business keys and expected update semantics before changing table write strategies.
3. Add source-batch duplicate counts before deduplication, then enforce key uniqueness on the prepared merge input.
4. Apply dataset-specific required-field, type, and domain checks; count and report rejected records rather than silently losing them.
5. Move workspace paths and storage account settings into environment configuration.
6. Validate representative input/output counts and rerun behavior in an available Spark/Databricks environment.

## Status

The repository review is complete. The findings above are code/configuration observations. Runtime transformation, schema compatibility, and end-to-end reconciliation remain environment-dependent checks.
