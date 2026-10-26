# Operations and Reliability Guide
## Uber Eats Azure Data Engineering Platform

## 1. Purpose

This guide defines the operational practices needed to run, monitor, troubleshoot, and recover the Uber Eats data engineering workflow.

The platform contains REST API, CSV, and SQL ingestion assets, Azure Data Factory resource definitions, Azure Data Lake Storage integration, and Databricks Silver-layer notebook exports.

The objective is to make pipeline operations predictable and data correctness measurable.

**Implementation boundary:** This document describes operational procedures and target reliability controls. A control is not considered implemented merely because it is documented. Confirm its implementation in the corresponding pipeline, notebook, configuration, or execution evidence before representing it as an existing capability.

## 2. Operational Principles

The platform should follow these principles:

1. **Repeatability:** Re-running a pipeline should not silently duplicate data.
2. **Recoverability:** A failed run should have a defined recovery procedure.
3. **Data correctness:** Successful execution alone does not prove that the output is correct.
4. **Traceability:** Every operational run should be identifiable and diagnosable.
5. **Controlled progress:** Incremental checkpoints should advance only after the required work has succeeded.
6. **Least privilege:** Each component should have only the permissions it needs.
7. **Evidence-based operation:** Recovery and completion decisions should use logs, checkpoints, output data, and reconciliation results.

## 3. Pipeline Operational Model

The logical workflow contains three ingestion patterns.

| Pipeline | Input | Operational focus |
|---|---|---|
| API ingestion | Payments, deliveries, ratings, and order-status endpoints | Authentication, pagination, timestamps, and source availability |
| CSV ingestion | Drivers, promotions, and delivery zones | File availability, schema consistency, and row counts |
| SQL incremental ingestion | Relational source and control configuration | Extraction boundaries, checkpoint management, and duplicate prevention |
| Silver transformations | Source or landing data | Schema enforcement, cleansing, business rules, and output correctness |

The exact dependency order and storage paths are determined by the deployed pipeline activities and environment configuration.

## 4. Failure and Recovery Matrix

| Failure scenario | Detection signal | Recommended response |
|---|---|---|
| API authentication failure | HTTP 401 or 403 | Confirm the configured credential and permissions; do not retry indefinitely |
| API rate limit or transient service failure | HTTP 429 or transient 5xx response | Apply bounded retries and respect the source's retry guidance |
| API response contains malformed data | Parsing or schema error | Preserve diagnostic details, quarantine or reject invalid records according to policy, and investigate |
| CSV source missing | File-not-found or activity failure | Confirm source delivery and path; rerun after the input becomes available |
| CSV schema changes | Column or type mismatch | Stop unsafe processing, review the schema change, and update validation deliberately |
| SQL extraction fails | Query or connectivity error | Resolve the underlying failure and resume from a known-safe extraction boundary |
| Watermark advances prematurely | Missing records after a failed run | Restore the last valid checkpoint and replay the affected interval safely |
| Databricks transformation fails | Notebook exception or failed job | Identify the failing transformation, correct the cause, and rerun using a safe write strategy |
| Source and target counts differ | Reconciliation discrepancy | Determine whether filtering, duplicates, rejected records, or partial processing explains the difference |
| Storage or permissions failure | Access denied or write failure | Check identity, permissions, destination path, and resource availability |

These responses are recommended operational controls. The actual retry behavior, quarantine mechanism, and checkpoint implementation must be established from the relevant resources.

## 5. Incremental Loading and Watermarks

Incremental extraction must define exactly which source records belong to a run.

A timestamp-based extraction may use a lower boundary, an upper boundary, or both. The chosen approach should be explicit and consistent across runs.

### Recommended bounded-window approach

For a run with a previously committed watermark \(W\), capture an upper boundary \(U\) and extract the intended interval.

For an inclusive lower boundary:

\[
W \leq \text{source\_timestamp} < U
\]

An inclusive lower boundary requires a deduplication or idempotent merge strategy. Alternatively, a strictly greater-than boundary can be used when the source guarantees that its timestamp semantics are sufficient to prevent missed records.

The upper boundary should be held constant for the run. This helps prevent a changing source from making the extraction interval ambiguous.

### Checkpoint advancement

The recommended sequence is:

1. Read the last successfully committed checkpoint.
2. Capture the run's extraction boundary.
3. Extract the required source interval.
4. Write the extracted data safely.
5. Validate the resulting output.
6. Perform required reconciliation.
7. Commit the checkpoint only after the required success conditions are satisfied.

If a run fails before the checkpoint is committed, recovery should replay the affected interval without creating duplicates.

### Late-arriving updates

A record can be updated after the pipeline has passed its original timestamp boundary. A lookback interval, source change-tracking mechanism, or composite cursor may help capture these changes.

The appropriate strategy depends on source guarantees, timestamp precision, update behavior, and the cost of reprocessing.

**Project-specific note:** The mock API filters using `updated_after`. That is a useful incremental

## 8. Reconciliation and Snapshot Completeness

The merge-based Drivers, Promotions, and Zones transformations use `reconcile_merge_target()` for source-key coverage and selected field-level comparisons. A separate `reconcile_full_snapshot()` utility is available for a confirmed complete snapshot. It compares source-only and target-only business keys, row counts, and optional configured aggregates, then writes detailed audit rows to `ubereats_databricks1.silver.pipeline_reconciliation_details`.

The utility requires the caller to pass `source_is_full_snapshot=True`. Do not enable it for incremental batches: a target key absent from a batch may be valid historical data, not a stale record. The utility is non-destructive; it reports target-only keys and does not delete them. Before integrating it into a pipeline, document the source delivery contract and confirm whether the extract is a full snapshot. Configure aggregate comparisons only for stable, semantically comparable columns. Failure events can reuse the same `run_id` as reconciliation details so operators can correlate the failed check with its error event. Failure-event logging is best-effort: if the event table cannot be written, the original pipeline failure remains the primary exception. These controls are implemented in repository code but have not been executed against a live Databricks/Delta workspace.

## 9. Operational SQL Reports

Use the repository's read-only SQL reports as a first-pass operational review after the audit Delta tables have been created:

- [Operational health report](OPERATIONAL_HEALTH_REPORT.sql): latest quality and merge-reconciliation outcomes, failures in the last 24 hours, and rejected-row volume.
- [Data-quality trends and incident analysis](DATA_QUALITY_TRENDS_AND_INCIDENTS.sql): daily quality outcomes, reconciliation misses and mismatches, failure-event counts, rejected-row counts, and day-over-day changes across the current date and previous seven calendar days.
- [Reconciliation audit queries](RECONCILIATION_AUDIT_QUERIES.sql): detailed failed checks, failure-event correlation, quarantine summaries, and recent merge-reconciliation outcomes.

Interpretation safeguards:

1. A missing quality or reconciliation row means no result was recorded in that table; it does not prove a successful run.
2. Failure-event counts are event counts, not deduplicated incidents. These reports do not calculate incident duration or time-to-resolution.
3. Quarantine counts are rejected-record events; they are not necessarily counts of distinct source entities.
4. A report's SQL and repository references have been reviewed, but live Databricks execution is pending an available workspace. Treat the output as an operational query pack, not evidence that the current cloud environment is running.

## 10. Notification Endpoint Security

The SQL incremental pipeline export intentionally does not contain a live Logic App callback URL. Configure notifications during deployment using a secret-backed design, such as retrieving the callback URL from Azure Key Vault with an appropriately authorized managed identity. Do not commit callback signatures, SAS tokens, API keys, or other credentials to source control.

A callback URL that was previously committed must be treated as exposed. Rotate or regenerate the Logic App trigger URL/signature in Azure; removing it from the current file does not erase it from Git history or invalidate the old token. Re-enable the notification activity only after the replacement endpoint is stored and referenced securely.

Because the Azure subscription may be disabled, perform the rotation and deployment configuration when access is available. The core SQL ingestion pipeline remains separate from the notification integration.

