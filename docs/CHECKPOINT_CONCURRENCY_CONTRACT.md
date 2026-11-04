# Checkpoint Concurrency and Compare-and-Swap Contract

## 1. Purpose

This contract defines how incremental ingestion runs coordinate access to a source control row and when a pipeline is allowed to advance its committed watermark.

A checkpoint is shared mutable state. If two runs read the same prior value and independently process different extraction windows, an unprotected update can cause lost progress, skipped data, or inconsistent recovery. The checkpoint must therefore be treated as a concurrency-sensitive commit record.

This document reviews the committed ADF pipeline definition and defines the required behavior for safe operation. It does not claim that the decommissioned Azure environment is currently available for live concurrency testing.

## 2. Finding in the exported ADF pipeline

The exported SQL incremental pipeline includes:

- `properties.concurrency: 1`, which limits concurrent runs of this pipeline when deployed and honored by Azure Data Factory.
- A checkpoint update predicate matching both `control_id` and the previous `last_watermark_value`.
- A SQL error (51001) when the update affects anything other than exactly one row.

This is a compare-and-swap-style optimistic concurrency check: the update succeeds only if the control row still contains the value observed by the run.

The pipeline-level concurrency setting is a global serialization guard for this pipeline. It is not a per-source lock and does not protect the control table from other pipelines, manual updates, or external writers. The compare-and-swap predicate helps detect a stale value at commit time, but it does not prevent duplicate work or make downstream writes transactional.

These are findings from the exported configuration, not evidence that a race condition has occurred or that concurrency behavior has been live-tested in the current environment.

## 3. Required checkpoint invariant

For each source control row, only a valid, successfully processed extraction window may advance the committed checkpoint.

A run may commit its captured upper watermark only when all of the following are true:

1. It read and retained the prior committed checkpoint.
2. It captured a finite upper watermark for the extraction window.
3. The bounded extraction completed successfully.
4. Required downstream transformation and validation completed successfully.
5. Reconciliation passed for the scope required by the source contract.
6. The control row still contains the prior checkpoint observed by this run.
7. The checkpoint update affected exactly one row.

If the prior checkpoint no longer matches, treat the operation as a concurrency conflict. Do not force the update or overwrite the newer checkpoint. Re-read current state, inspect the run's landing and target writes, reconcile the affected interval, and retry only under the documented replay-safe policy.

## 4. Compare-and-swap SQL pattern

The following is a conceptual SQL Server pattern. Adapt parameter types, precision, schema, and null behavior to the actual control-table definition. Bind values using typed parameters; do not concatenate untrusted values into SQL text.

```sql
BEGIN TRANSACTION;

UPDATE dbo.pipeline_control
SET last_watermark_value = @captured_upper_watermark
WHERE control_id = @control_id
  AND last_watermark_value = @previous_watermark;

IF @@ROWCOUNT <> 1
BEGIN
    ROLLBACK TRANSACTION;
    THROW 51001, 'Checkpoint conflict: control row changed or was not unique.', 1;
END;

COMMIT TRANSACTION;
```

Important implementation details:

- The control dataset now declares `last_watermark_value` as `datetime2(7)`. Apply the [precision migration](../sql/migrations/20261104_widen_pipeline_checkpoint_to_datetime2_7.sql) before deploying the updated ADF dataset. It preserves existing nullability and checkpoint values; it has not been run against the retired SQL environment.

- `@control_id`, `@previous_watermark`, and `@captured_upper_watermark` must use types compatible with the actual table columns.
- Timestamp precision and timezone interpretation must be consistent between source values, ADF expressions, and SQL storage.
- The control table should enforce uniqueness for `control_id`; the row-count check is a runtime guard, not a replacement for a unique constraint.
- A null prior checkpoint needs a separate explicit branch using `IS NULL` and an approved initial-load strategy. Ordinary equality does not match `NULL`.
- If the ADF Script activity cannot safely bind typed parameters for the required expression, use a stored procedure or another parameterized implementation supported by the deployed connector.
- A successful SQL update is the checkpoint commit point; it does not retroactively make earlier lake or Spark writes atomic with the control-table transaction.

## 5. Concurrency-control options

Choose a policy that matches the expected workload and document its scope.

### Option A — Serialize pipeline execution

Prevent overlapping runs of the relevant pipeline.

- Simple to reason about.
- Can protect all source rows processed by that pipeline from overlapping executions of the same pipeline.
- May reduce throughput when independent sources could safely run in parallel.
- Does not prevent another pipeline or external process from changing the control row.

The exported pipeline's concurrency setting of one follows this broad serialization approach.

### Option B — Per-source lease or lock

Acquire a lock for a specific source/control row before reading its checkpoint.

- Allows independent sources to process concurrently.
- Requires lease ownership, expiry, renewal, release, and abandoned-lock recovery rules.
- Must handle a worker failing after acquiring the lease.
- Does not replace replay-safe destination writes.

### Option C — Optimistic compare-and-swap

Allow work to overlap, but permit a checkpoint commit only if the prior value is still current.

- Avoids holding a lock throughout long extraction and transformation work.
- Detects stale checkpoint commits.
- Can cause wasted work when a run loses the commit race.
- Requires safe reconciliation and replay of any outputs already written by the losing run.

A compare-and-swap update alone is not a complete concurrency strategy. The landing and Silver write paths must also tolerate retries and duplicate processing.

## 6. Failure and recovery behavior

| Scenario | Required checkpoint behavior | Recovery action |
|---|---|---|
| Control lookup fails | Do not advance | Investigate source/control configuration |
| Bounded copy fails | Do not advance | Inspect partial landing output before retry |
| Silver processing fails | Do not advance | Inspect partial Silver writes and reconcile |
| Validation or required reconciliation fails | Do not advance | Quarantine or repair records according to source rules |
| Compare-and-swap affects zero rows | Fail visibly; do not force update | Re-read checkpoint and reconcile run outputs |
| Compare-and-swap affects multiple rows | Fail visibly; do not continue | Investigate control-row uniqueness/data integrity |
| External writer changes the checkpoint | Reject stale commit | Coordinate with the other writer and reconcile |
| Full processing and checkpoint commit succeed | Commit captured upper watermark | Next run starts from committed state, subject to source contract |

A checkpoint remaining unchanged after a failure is necessary but not sufficient for safe recovery. The failed run may have already created landing files or partially updated target tables. Recovery must account for those side effects.

## 7. Required observability

For each logical extraction run, capture enough information to explain and reconcile its checkpoint decision:

- Source/control identifier.
- Run ID and pipeline/activity identifiers.
- Checkpoint value read at run start.
- Captured upper watermark and extraction interval.
- Copy, transformation, validation, and reconciliation outcomes.
- Rows read, landed, rejected/quarantined, and written where available.
- Checkpoint update result and affected row count.
- Final checkpoint value.
- Conflict/failure reason and recovery action.

Avoid logging credentials, secrets, or sensitive source payloads.

## 8. Acceptance tests

When a suitable Azure test environment is available, execute and retain evidence for:

1. Two runs start from the same prior checkpoint and attempt to commit different upper bounds; a stale commit must fail.
2. A successful run commits exactly one control row.
3. A control ID matching zero rows fails visibly.
4. A control ID unexpectedly matching multiple rows fails visibly.
5. A failed copy does not advance the checkpoint.
6. A failed Silver job does not advance the checkpoint.
7. A validation/reconciliation failure does not advance the checkpoint.
8. A null initial checkpoint follows the explicit initial-load branch.
9. An external checkpoint update causes the stale run to fail its compare-and-swap.
10. A losing run's partial landing and Silver outputs can be reconciled and safely replayed.
11. Independent source rows behave according to the chosen serialization/locking policy.
12. Timestamp precision and type conversion preserve the expected comparison semantics.

For each test, preserve the input scenario, run IDs, old and captured checkpoint values, affected-row count, final checkpoint, expected result, actual result, and evidence location. Mark tests not run when no runtime evidence exists.

## 9. Current implementation status

The committed ADF export declares pipeline concurrency as one and includes a compare-and-swap-style predicate in the checkpoint update, with SQL error 51001 when the update row count is not exactly one. The checkpoint update is ordered after successful copy and Silver processing. Before source extraction, the pipeline now validates the configured schema/table/watermark column against SQL catalog metadata and fails when source rows contain null watermark values. Source queries honor the configured schema and use bracket-quoted identifiers after the preflight.

The control-dataset contract now uses `datetime2(7)`, with a migration script that preserves the physical control column's nullability and existing checkpoint values. The migration must be applied and verified before deploying the updated ADF dataset; it has not been run against the retired Azure SQL environment.

The exported definition still requires source-specific handling for null starting control checkpoints, late/equal watermark visibility, deletes, and replay-safe Silver merges. The concurrency setting does not protect against external writers, and checkpoint compare-and-swap does not establish idempotency for landing or Silver writes.

The project owner confirms that the platform was successfully executed end to end before its Azure resources were decommissioned to control costs. The current repository can be inspected and its available automated checks can be run, but the decommissioned environment is not available for a new live Azure concurrency acceptance cycle. Do not present configuration inspection as a new runtime test.

## 10. Related documentation

- [SQL Incremental Load Engineering Review](SQL_INCREMENTAL_LOAD_REVIEW.md)
- [Incremental Source Contract](INCREMENTAL_SOURCE_CONTRACT.md)
- [Recovery and Rerun Runbook](RECOVERY_AND_RERUN_RUNBOOK.md)
