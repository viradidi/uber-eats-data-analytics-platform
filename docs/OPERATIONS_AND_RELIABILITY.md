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