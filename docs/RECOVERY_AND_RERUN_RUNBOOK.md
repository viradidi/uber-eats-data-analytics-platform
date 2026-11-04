# Recovery and Rerun Runbook
## Uber Eats Azure Data Engineering Platform

## Purpose and execution context

This runbook defines how to investigate failed ingestion and transformation runs, assess partial writes, decide whether a rerun is safe, and verify the recovered data.

The project owner confirms that the platform was completed and successfully executed end to end. The Azure resources/environments were subsequently decommissioned to control costs, so there is no live deployment available for an operator to inspect today. The procedures below document the project's intended operational controls; apply them only after provisioning and validating an environment that matches the committed configuration. Historical successful execution should not be confused with current resource availability.

## 1. First response: preserve evidence

For every failed run:

1. Capture the pipeline/job name, run ID, start and end timestamps, failing activity or notebook task, error text, source dataset, and parameters.
2. Record the last committed checkpoint/watermark before attempting a rerun. Do not edit it merely to make a run progress.
3. Identify the last confirmed successful stage: source read, Bronze landing, Silver validation, target write/merge, reconciliation, or checkpoint update.
4. Determine whether the failed attempt produced partial effects. An orchestrator failure does not prove that no files or rows were written.
5. Preserve the original error and relevant quality, reconciliation, failure-event, and quarantine records. Do not clear audit data to make a run appear healthy.

If the cause is unavailable infrastructure, missing credentials, or an inaccessible dependency, restore that dependency before retrying. Repeated reruns do not repair unavailable services.

## 2. Select a recovery path

| Failure stage | Investigate and recover | Avoid |
|---|---|---|
| Source lookup/read | Check source availability, permissions, schema, query parameters, and cursor boundaries; retry after correcting the cause. | Advancing a watermark because the source could not be read. |
| Bronze/landing write | Inspect landed files/rows, naming, range, and write semantics. Establish whether repeating the same range overwrites, appends, or creates a new object before rerunning. | Assuming a failed activity wrote nothing or assuming deterministic filenames guarantee idempotency. |
| Silver validation | Review rejected records and validation messages; correct source data or an approved rule, then rerun the affected scope. | Disabling checks or silently dropping rejected records to force success. |
| Duplicate business keys | Inspect the full quarantined key group and establish an explicit, deterministic source-specific resolution rule. | Choosing an arbitrary winner or relying on unstable row order. |
| Delta merge/reconciliation | Inspect target state, merge keys, run ID, missing keys, mismatched fields, and whether the write committed. Retry only after confirming replay safety. | Deleting target data or running a full refresh without an approved recovery plan. |
| Checkpoint update | Verify all required downstream work and reconciliation succeeded; inspect the current checkpoint before retrying the update. | Moving the checkpoint past source changes that have not been durably processed. |
| Notification failure | Diagnose notification credentials and endpoint separately from the core data path; determine whether the notification is retryable. | Committing callback URLs, signatures, SAS tokens, or API keys to source control. |

## 3. SQL incremental pipeline recovery

The exported ADF definition is `pipeline/sql_incremental pipeline.json`. It reads control metadata from `pipeline_control` and uses configured source-table and watermark metadata.

1. Inspect the affected control row: source table, watermark column, and last committed watermark.
2. Confirm the source table and watermark column exist and that their data type, precision, and timezone semantics match the pipeline configuration.
3. Establish the intended extraction interval. The implemented bounded pattern is conceptually `watermark > previous_checkpoint AND watermark <= captured_upper_bound`.
4. Inspect the run to establish whether the copy/landing activity completed and whether the downstream Silver job succeeded.
5. Reconcile the landed and curated data against the expected source range using stable business keys and comparable fields.
6. Advance the checkpoint only after the intended data has been persisted and all required downstream success conditions have been met.
7. If replay is needed, use a bounded range where supported. For sources that can expose late updates or repeated cursor values, use a deliberately designed overlap/replay strategy together with idempotent destination handling. Do not widen the range blindly.

### Checkpoint concurrency safeguards

The committed pipeline export sets pipeline concurrency to one and uses a compare-and-swap style control-row update based on the control ID and previously read watermark. The SQL update raises error 51001 when the affected-row count is not exactly one. These controls reduce conflicting updates within the intended pipeline execution path; they do not provide a global lock across other pipelines or external writers, and they do not make landing or Silver writes idempotent.

If the compare-and-swap update fails, reread the control row and inspect the competing run or writer. Do not simply rerun the old update with a newly observed watermark. Reconcile the destination first, then determine the correct checkpoint from the data actually committed.

The bounded upper watermark helps define a finite extraction window, but it does not by itself capture late-arriving rows whose cursor is at or below an already committed checkpoint, deletes, or updates that retain an older cursor. These cases require a source-specific overlap, CDC/tombstone, or reconciliation policy. Treat null/uninitialized checkpoints as an explicit initial-load case rather than assuming a normal incremental predicate handles them.

## 4. Silver notebook recovery

### Drivers, Promotions, and Zones

1. Inspect `pipeline_failure_events`, `pipeline_reconciliation_results`, and `pipeline_rejected_records` for the dataset and run.
2. For duplicate keys, review every quarantined row. The implemented policy is to quarantine the duplicate-key group and fail before merge; it does not automatically select a winner.
3. Correct the source or approve a deterministic winner rule before retrying the affected batch.
4. Confirm the target merge key and the source columns used by reconciliation.
5. Rerun the affected transformation and confirm the latest quality result and reconciliation status are successful.

### Payments, Deliveries, Order Status, and Ratings

1. Check the validation error and quarantine reason.
2. Confirm the flattened API payload is non-empty before retrying. Empty-payload guards are intended to prevent accidental replacement of populated targets with empty data.
3. Check required columns, null fields, uniqueness rules, and dataset-specific numeric/domain constraints.
4. Retry only after correcting the source or an approved transformation rule. Confirm target row-count reconciliation and successful quality metrics afterward.

A successful notebook task alone is not proof of correctness. Verify durable audit records and target data for the processed scope.

### Cumulative API snapshot replay

The API ingestion pipeline writes timestamped endpoint files, and the API Silver notebooks read the whole endpoint folder. Before publishing, those notebooks now call `src/common/incremental.py` to collapse repeated snapshots:

- Payments, deliveries, and ratings retain the record with the greatest source `updated_at` per business key.
- Order-status events use `(order_id, status_timestamp)` as their event key and collapse only exact repeats.
- Conflicting payloads with the same key and source version, null keys, or invalid versions fail before the Silver overwrite.

When investigating a failure, preserve the landing files and inspect the rejection message. Do not delete old snapshots to make the run pass. Correct the source conflict or approved version contract, rerun the endpoint processing, and verify the Silver target key counts and business fields.

This protection applies to accumulated API snapshots. The SQL incremental pipeline still has a strict lower watermark and does not automatically recover equal/older late-arriving cursor values or hard deletes. Use its source contract and a verified source-specific overlap/CDC/reconciliation strategy; do not assume API snapshot deduplication solves the SQL cursor problem.

## 5. Checkpoint advancement gate

Advance a watermark/checkpoint only when all applicable conditions are true:

- The source range was read successfully and its boundaries are known.
- The intended rows were durably persisted to the destination.
- Required downstream transformations completed.
- Validation and reconciliation passed, or an explicitly permitted exception was documented and approved.
- Audit records can be correlated to the run.
- Replaying the same range will not create duplicate business records or corrupt target state.

If a required audit write failed, do not invent a successful status. Resolve the observability failure or record the operational exception through the approved incident process before advancing the checkpoint.

## 6. Rerun acceptance checklist

Before closing a recovery, verify:

- [ ] Root cause is identified, or remaining uncertainty is documented.
- [ ] Source range and checkpoint before recovery are recorded.
- [ ] Partial landing and target writes have been inspected.
- [ ] Rerun did not create duplicate business keys.
- [ ] Expected source and target row counts reconcile for the processed scope.
- [ ] Relevant business fields match for the processed keys.
- [ ] Latest quality metrics show the expected outcome.
- [ ] Reconciliation has no unexplained missing keys or field mismatches.
- [ ] Quarantined records have an owner and disposition; none were silently discarded.
- [ ] Checkpoint advanced only after required work succeeded.
- [ ] Incident notes include the run ID, cause, corrective action, and remaining follow-up.

## 7. Evidence, audit, and limitations

The repository includes quality-result, failure-event, rejected-record, merge-reconciliation, and full-snapshot reconciliation utilities, together with read-only SQL audit reports. Use the full-snapshot helper only when the source contract confirms a complete snapshot. An incremental batch must not be interpreted as proof that target keys absent from that batch are stale or deleted.

This runbook does not claim automated rollback, automatic checkpoint restoration, exactly-once processing, or a currently running Azure deployment. A deterministic landing filename is not, by itself, proof of idempotent writes. Validate actual sink semantics, merge behavior, and checkpoint coordination in any redeployed environment.

## 8. Repository-level safeguards

The GitHub Actions workflow runs the standard-library test suite with `python -m unittest discover -s tests -v`. The `tests/test_recovery_contract.py` tests assert that the committed ADF JSON preserves intended dependency gates:

- The bounded max-watermark lookup succeeds before the copy activity.
- The Silver Databricks job succeeds after the copy activity.
- The control watermark update depends on successful completion of the Silver job.
- Recovery documentation retains checkpoint-protection requirements and accurately distinguishes static checks from runtime behavior.
- Operational SQL report files remain present and linked from the operations guide.

These are static contract tests. They can catch accidental changes to exported JSON or documented safeguards, but they do not execute ADF expressions, contact Azure, validate credentials, or prove that a future deployment matches the export. A green CI run is not a substitute for a controlled runtime recovery test.

## Related operational contracts

- [SQL incremental load review](SQL_INCREMENTAL_LOAD_REVIEW.md)
- [Incremental source contract](INCREMENTAL_SOURCE_CONTRACT.md)
- [Checkpoint concurrency contract](CHECKPOINT_CONCURRENCY_CONTRACT.md)
- [Architecture overview](../architecture/architecture.md)
