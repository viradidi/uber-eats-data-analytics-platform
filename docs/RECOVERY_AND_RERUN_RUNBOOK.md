# Recovery and Rerun Runbook
## Uber Eats Azure Data Engineering Platform

## Purpose and operating boundary

This runbook gives an operator a repeatable sequence for diagnosing a failed ingestion or Silver transformation, deciding whether a rerun is safe, and checking data correctness afterward.

The Azure subscription is not currently available for live validation. These are repository-level operating procedures, not evidence of a running cloud deployment. Before executing them, confirm the workspace, linked services, secrets, target tables, and pipeline parameters match the deployed environment.

## 1. First response: preserve evidence

For every failed run:

1. Capture the pipeline/job name, run ID, start/end timestamps, failing activity or notebook task, error text, and source dataset.
2. Record the last successful checkpoint/watermark before attempting a rerun. Do not edit it merely to make a run progress.
3. Identify the last successful stage: source read, Bronze landing, Silver validation, target write/merge, reconciliation, or checkpoint update.
4. Check whether the failed attempt wrote any output before failing. A failed run may have partial effects even when the orchestrator reports failure.
5. Preserve the original error and relevant quality, reconciliation, failure-event, and quarantine records. Do not clear audit tables to make the dashboard appear healthy.

If the failure is caused by missing credentials, unavailable storage, or disabled cloud resources, stop and restore access/configuration first. Repeated reruns will not fix an unavailable dependency.

## 2. Choose the correct recovery path

| Failure stage | Safe response | Do not do |
|---|---|---|
| Source lookup/read | Check source availability, permissions, schema, and query parameters; retry after the cause is corrected. | Advance a watermark because the source could not be read. |
| Bronze write | Inspect whether files/rows landed and whether the write is repeatable. Rerun only with the pipeline's known idempotency behavior understood. | Assume a failed activity wrote nothing. |
| Silver validation | Review rejected records and validation message; correct source data or an approved rule, then rerun the affected dataset. | Disable validation or silently drop rejected records to force success. |
| Duplicate business keys | Inspect all rows in the quarantined key group and establish an explicit source-specific winner rule or correct the source. | Select an arbitrary row or use an unstable deduplication order. |
| Delta merge/reconciliation | Check target state, run ID, missing keys, mismatched fields, and whether the merge partially committed. Rerun only after confirming merge idempotency. | Delete target data or run a full refresh without an approved recovery plan. |
| Checkpoint/watermark update | Verify that all required downstream work and reconciliation succeeded before updating/advancing the checkpoint. | Move the checkpoint forward past unprocessed source changes. |
| Notification failure | Diagnose notification credentials/endpoint separately from the core data path. | Commit callback URLs, signatures, SAS tokens, or API keys to source control. |

## 3. SQL incremental pipeline recovery

The committed ADF export is the file at pipeline/sql_incremental pipeline.json. Its control lookup reads pipeline_control, and the pipeline uses configured source table and watermark metadata to identify changes.

1. Inspect the control row for the affected table: source table, watermark column, and last committed watermark.
2. Confirm the source table and watermark column exist and that the watermark predicate uses the expected type/format.
3. Determine the actual source range that should be processed, including rows exactly at the boundary and any late-arriving updates.
4. Inspect the failed run to determine whether the copy/write stage completed and whether downstream Silver processing succeeded.
5. Reconcile the destination against the expected source range using stable business keys and comparable fields.
6. Advance the control watermark only after the intended copy and all required downstream success conditions are met.
7. Rerun a bounded range when supported by the pipeline; prefer a controlled overlap plus idempotent destination handling if the source can deliver late updates. Do not widen the range blindly without estimating duplicate/reprocessing impact.

The repository export has been reviewed for dependency ordering, but ADF expression behavior and the full recovery path have not been live-tested in Azure. Confirm the deployed activity graph and actual control-table update condition before applying a production recovery.

## 4. Silver notebook recovery

For Drivers, Promotions, and Zones:

1. Inspect pipeline_failure_events, pipeline_reconciliation_results, and pipeline_rejected_records for the dataset and run.
2. For duplicate keys, review every quarantined row. The implemented policy is to quarantine the duplicate-key group and fail before merge; no winner is selected automatically.
3. Correct the source or approve a deterministic winner rule before retrying the affected batch.
4. Confirm the target merge key and the source columns used by the reconciliation check.
5. Rerun the affected transformation, then confirm the latest quality result and reconciliation status are successful.

For Payments, Deliveries, Order Status, and Ratings:

1. Check the validation error and quarantine reason.
2. Confirm the flattened API payload is non-empty before retrying. Empty payload guards are intended to prevent accidental replacement of populated targets with empty data.
3. Check required columns, null fields, uniqueness rules, and dataset-specific numeric/domain constraints.
4. Retry only after the source or approved transformation rule is corrected; then confirm target row-count reconciliation and successful quality metrics.

Do not treat a successful notebook task alone as proof of correctness. Use the durable audit records and target checks.

## 5. Checkpoint advancement gate

Advance a watermark/checkpoint only when all applicable conditions are true:

- The source range was read successfully and its boundaries are known.
- The intended rows were persisted to the destination.
- Required downstream transformations completed.
- Validation and reconciliation passed, or any explicitly permitted exception was documented and approved.
- The run's audit records can be correlated by run ID.
- Repeating the same source range will not create duplicate business records or corrupt target state.

If a required audit table could not be written, do not invent a successful status. Resolve the observability failure or record the operational exception through the approved incident process before checkpoint advancement.

## 6. Rerun acceptance checklist

Before closing a recovery, verify:

- [ ] Root cause is identified or the remaining uncertainty is documented.
- [ ] Source range and checkpoint before recovery are recorded.
- [ ] Rerun did not create duplicate business keys.
- [ ] Expected source and target row counts reconcile for the processed scope.
- [ ] Relevant business fields match for the processed keys.
- [ ] Quality metrics show the latest outcome.
- [ ] Reconciliation has no unexplained missing keys or field mismatches.
- [ ] Quarantined records have an owner and disposition; none were silently discarded.
- [ ] Checkpoint advanced only after required work succeeded.
- [ ] The incident note includes the run ID, cause, corrective action, and remaining follow-up.

## 7. Evidence and limitations

The repository includes quality-result, failure-event, rejected-record, merge-reconciliation, and full-snapshot reconciliation utilities, plus read-only SQL audit reports. The full-snapshot helper must only be used when the source contract confirms a complete snapshot; incremental batches must not be interpreted as proof that absent target keys are stale.

This runbook does not claim automated rollback, automatic checkpoint restoration, exactly-once processing, or live Azure recovery unless those capabilities are independently demonstrated in the deployed environment.

## 8. Automated repository-level safeguards

The repository's GitHub Actions workflow runs the standard-library test suite with `python -m unittest discover -s tests -v`. The `tests/test_recovery_contract.py` tests assert that the committed ADF JSON keeps the intended dependency gates:

- The bounded max-watermark lookup succeeds before the copy activity.
- The Silver Databricks job succeeds after the copy activity.
- The control watermark update depends on successful completion of the Silver job.
- The recovery documentation continues to require checkpoint protection and disclose that live Azure behavior is unverified.
- The operational SQL report files remain present and linked from the operations guide.

These are static contract tests. They can catch accidental changes to the exported JSON or recovery safeguards, but they do not execute ADF expressions, contact Azure, validate credentials, or prove that deployed resources match the export. A green CI run is not a substitute for a controlled runtime recovery test.
