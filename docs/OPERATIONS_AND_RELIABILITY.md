# Operations and Reliability Guide
## Uber Eats Azure Data Engineering Platform

## 1. Purpose and execution context

This guide describes the operational model for ingestion, transformation, data-quality checks, reconciliation, monitoring, and recovery across the Uber Eats data platform.

The project owner confirms that the platform was completed and successfully executed end to end. Its Azure environments were later decommissioned to control costs; there is no current live deployment to inspect. This guide documents the project's implemented repository controls and recommended operating practices. It does not claim that the cloud resources are currently running or that every documented operational control was independently audited.

## 2. Operational principles

1. **Repeatability:** Replaying a source range must have understood destination-write semantics.
2. **Recoverability:** Every failure should leave enough evidence to identify the last safe stage.
3. **Data correctness:** A successful activity is not sufficient proof of correct output.
4. **Traceability:** Correlate pipeline runs, notebook work, quality results, reconciliation, and failure events.
5. **Controlled progress:** Advance incremental checkpoints only after required downstream work succeeds.
6. **Least privilege:** Keep credentials out of source control and grant each component only the access it needs.
7. **Honest observability:** Missing audit rows are unknown outcomes, not successful outcomes.

## 3. Logical pipeline operating model

| Workload | Inputs | Main operational concerns |
|---|---|---|
| API ingestion | Payments, deliveries, ratings, and order-status endpoints | API key, pagination, timestamp filtering, response validation, source availability |
| File ingestion | Drivers, promotions, and delivery zones CSV files | File arrival, schema consistency, parsing, row counts |
| SQL incremental ingestion | Relational source plus `pipeline_control` metadata | Bounded extraction window, cursor semantics, checkpoint coordination |
| Silver transformations | Landed API, file, and SQL data | Schema standardization, validation, quarantine, merge/write behavior, reconciliation |

Azure Data Factory orchestrates ingestion activities; ADLS Gen2 is the landing/storage layer; Databricks and PySpark implement Silver transformations and shared processing utilities. Exact resource names, paths, identities, and linked-service settings are environment-specific.

## 4. Monitoring signals and audit evidence

Use the available repository audit assets to build a run-level view. Correlate records using dataset/table identifiers, timestamps, and the run ID wherever the relevant code path supplies one.

| Signal | What it tells the operator | Important limitation |
|---|---|---|
| ADF activity/pipeline status | Which orchestration stage failed or completed | A failed activity may have partial side effects |
| Databricks job/notebook result | Whether a transformation task completed | Task success alone does not prove data correctness |
| Quality results | Whether configured validation checks passed | No result row is not a pass |
| Rejected-record/quarantine details | Which records failed configured rules and why | Rejected events may not equal distinct source entities |
| Failure events | Recorded operational exceptions | Event logging is best-effort; a logging failure may leave no event |
| Merge reconciliation | Source-key coverage and selected field comparisons | Only checks the configured keys and compared fields |
| Full-snapshot reconciliation | Source-only/target-only keys, counts, and configured aggregates | Valid only when the input is a confirmed complete snapshot |
| Checkpoint/control row | Last committed incremental cursor | Must be interpreted with the destination state and run history |

The SQL report pack is intended for read-only operational review after the corresponding audit Delta tables exist. A missing report row should be investigated; never interpret absence as proof of success.

## 5. Data quality and reconciliation

The platform includes validation and quarantine utilities, merge reconciliation, and a separate full-snapshot reconciliation helper.

### Merge reconciliation

For merge-based Drivers, Promotions, and Zones transformations, `reconcile_merge_target()` checks source-key coverage and selected field-level comparisons. Review the detailed reconciliation output for missing keys or field mismatches, and correlate it with the corresponding run and failure-event records.

### Full-snapshot reconciliation

`reconcile_full_snapshot()` compares source-only and target-only business keys, row counts, and optional configured aggregates. The caller must explicitly pass `source_is_full_snapshot=True`. Do not use this helper for incremental batches: a key absent from one batch may be valid historical target data, not a deletion. The helper reports discrepancies and does not delete target rows.

Configure aggregate comparisons only for stable, semantically comparable columns. Confirm the source delivery contract before classifying an extract as a full snapshot.

### Quarantine and duplicate keys

For the Drivers, Promotions, and Zones merge path, duplicate-key groups are quarantined and cause processing to fail before merge; the implementation does not select an arbitrary winner. Review all rows in the group and apply an approved, deterministic resolution rule before retrying. Do not silently discard rejected records or weaken validation to force a green run.

## 6. Incremental loading and watermark management

The SQL incremental pipeline reads source metadata from `pipeline_control` and uses a bounded extraction pattern. Conceptually, it captures an upper watermark (U) and extracts rows satisfying:

[
W < \text{source\_watermark} \leq U
]

where (W) is the previously committed checkpoint. The upper bound remains fixed for the run, preventing the extraction interval from shifting while the source changes.

### Checkpoint advancement sequence

1. Read the last committed checkpoint and source metadata.
2. Capture the run's upper watermark.
3. Extract the bounded source interval.
4. Persist the extracted data to landing.
5. Complete required Silver processing.
6. Validate and reconcile the processed scope.
7. Advance the checkpoint only after the configured downstream success dependency is satisfied.

The exported ADF definition places the checkpoint update after successful completion of the Silver Databricks job. It also configures pipeline concurrency to one and uses a compare-and-swap style update against the previously read watermark, raising SQL error 51001 if the affected-row count is not exactly one.

These safeguards do not provide a global lock across other pipelines or external writers, and they do not independently make landing or Silver writes idempotent. A failed compare-and-swap should trigger inspection of the current control row, competing writers, and destination state—not an unconditional retry using a newly observed value.

### Cursor limitations and source contract

A strict lower-bound predicate can miss late-visible rows or updates that retain an older/equal cursor after the checkpoint has advanced. Deletes are not captured unless the source exposes CDC, tombstones, soft-delete flags, or another explicit deletion signal. Null/uninitialized checkpoints need an explicit initial-load strategy.

The mock API uses `updated_after` filtering. That supports incremental retrieval, but correctness still depends on the source's timestamp precision, update behavior, and visibility guarantees. See the [Incremental Source Contract](INCREMENTAL_SOURCE_CONTRACT.md) and [SQL Incremental Load Review](SQL_INCREMENTAL_LOAD_REVIEW.md) before onboarding a new source.

## 7. Failure handling and retry policy

| Failure scenario | Detection | Recommended response |
|---|---|---|
| API authentication failure | HTTP 401/503 from the mock API | Confirm the API key is configured and authorized; the API fails closed when the key is missing |
| API transient failure or rate limit | HTTP 429 or transient 5xx | Apply bounded retries consistent with source guidance; avoid indefinite retry loops |
| Malformed API payload | Parsing/schema validation error | Preserve diagnostics and quarantine/reject according to the dataset rules |
| CSV source missing | Missing file or failed copy | Confirm source delivery and path, then retry when input is available |
| CSV schema drift | Column/type mismatch | Stop unsafe processing, review the contract, and update validation deliberately |
| SQL extraction failure | Query or connectivity error | Resolve the cause and resume from a known-safe bounded interval |
| Partial landing/target write | Activity failure with possible side effects | Inspect actual destination state and establish replay behavior before retrying |
| Duplicate business keys | Quarantine or validation failure | Resolve the entire duplicate group with a deterministic rule |
| Databricks transformation failure | Failed job/notebook | Identify the failing transformation, inspect target state, correct the cause, and rerun safely |
| Reconciliation discrepancy | Missing keys or mismatched fields | Determine whether filtering, rejected records, partial processing, or source changes explain it |
| Storage/permission failure | Access denied or write failure | Check identity, permissions, destination path, and resource availability |
| Notification failure | Callback activity failure | Diagnose notification configuration separately from the core data path |

Use the [Recovery and Rerun Runbook](RECOVERY_AND_RERUN_RUNBOOK.md) for the detailed incident sequence, partial-write investigation, rerun gate, and closure checklist. Never advance a checkpoint simply because an orchestration run failed or because a retry is needed.

## 8. Operational SQL report pack

Use the repository's read-only SQL reports after the audit Delta tables have been created:

- [Operational health report](OPERATIONAL_HEALTH_REPORT.sql): latest quality and merge-reconciliation outcomes, failures in the last 24 hours, and rejected-row volume.
- [Data-quality trends and incident analysis](DATA_QUALITY_TRENDS_AND_INCIDENTS.sql): daily quality outcomes, reconciliation misses and mismatches, failure-event counts, rejected-row counts, and day-over-day changes across the current date and previous seven calendar days.
- [Reconciliation audit queries](RECONCILIATION_AUDIT_QUERIES.sql): detailed failed checks, failure-event correlation, quarantine summaries, and recent merge-reconciliation outcomes.

Interpret these reports carefully:

1. A missing quality or reconciliation row means no result was recorded in that table; it does not prove a successful run.
2. Failure-event counts are event counts, not deduplicated incidents. The reports do not calculate incident duration or time to resolution.
3. Quarantine counts represent rejected-record events and may not equal distinct source entities.
4. Reports are operational query assets; their presence in Git does not imply a currently running Databricks workspace.

## 9. Notification and secret security

The SQL incremental pipeline export does not contain a live Logic App callback URL. Configure notifications during deployment using a secret-backed design, such as retrieving the callback URL from Azure Key Vault with an appropriately authorized managed identity.

Do not commit callback signatures, SAS tokens, API keys, or other credentials to source control. If a callback URL was previously committed, treat it as exposed: rotate or regenerate the trigger URL/signature in Azure. Removing it from the current file does not erase Git history or invalidate the old token.

Keep notification integration separate from the core ingestion success path. A notification failure should be investigated independently and must not cause an unsafe checkpoint advancement.

## 10. Repository validation and runtime boundary

The repository's GitHub Actions workflow performs repository-level checks, including JSON/notebook validation, Python and notebook code-cell syntax checks, dependency installation, unit tests, and API security configuration checks. These tests help detect regressions in committed assets; they do not execute Azure Data Factory expressions, contact Azure services, validate live credentials, or prove that a future deployment matches the repository.

The project owner confirms that the platform previously ran end to end. Azure resources were later decommissioned to control costs. A redeployment should therefore include environment-specific configuration, secret provisioning, identity/permission checks, controlled source-to-target validation, and a supervised runtime recovery test before production use.

## Related operational contracts

- [Architecture overview](../architecture/architecture.md)
- [Recovery and rerun runbook](RECOVERY_AND_RERUN_RUNBOOK.md)
- [SQL incremental load review](SQL_INCREMENTAL_LOAD_REVIEW.md)
- [Incremental source contract](INCREMENTAL_SOURCE_CONTRACT.md)
- [Checkpoint concurrency contract](CHECKPOINT_CONCURRENCY_CONTRACT.md)
