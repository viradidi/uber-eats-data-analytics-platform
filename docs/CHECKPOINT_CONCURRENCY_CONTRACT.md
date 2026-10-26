# Checkpoint Concurrency and Compare-and-Swap Contract

## Finding in the exported ADF pipeline

The exported `update watermark_copy1` activity updates `dbo.pipeline_control.last_watermark_value` using `control_id` alone and does not condition the update on the prior watermark observed by this run. The exported pipeline now declares `properties.concurrency: 1`, which serializes runs of this pipeline when that definition is deployed and honored by ADF. This is a conservative global guard, not a per-source lock, and it does not protect against other pipelines or external writers updating the same control table.

This is a configuration finding, not proof that overlapping runs have occurred. The Azure deployment is not available for live validation.

## Required invariant

For each source control row, at most one logical extraction window may commit a checkpoint at a time. A run may advance the checkpoint only if:

1. It captured the previous checkpoint and a bounded upper watermark.
2. Its copy and required Silver processing succeeded.
3. Its source-to-landing and target reconciliation passed.
4. The control row still has the previous checkpoint this run observed.
5. The checkpoint update affected exactly one row.

If the previous checkpoint no longer matches, treat the run as a concurrency conflict. Do not force the update; re-read the checkpoint, reconcile any already-written landing/target output, and retry only with the documented replay-safe strategy.

## Compare-and-swap SQL pattern

The following is a conceptual SQL Server pattern. Adapt the parameter types to the actual watermark column and control-table schema; do not paste string placeholders into a live query. Use typed parameters in the implementation, not string concatenation.

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

The equality comparison must use the real watermark type and precision. Null starting checkpoints require a separate, explicit branch (`IS NULL`) and a defined initial-load strategy; ordinary equality does not match `NULL`. If the current ADF Script activity cannot safely bind typed parameters, use a stored procedure or another parameterized implementation rather than embedding untrusted values in SQL text.

## Prevent overlapping writers

Choose and document one enforced policy:

- **Serialize the pipeline:** configure orchestration so a second run cannot overlap the first for the same source control row; or
- **Use a per-source lease/lock:** acquire it before reading the checkpoint and release it safely on success/failure; or
- **Use optimistic compare-and-swap:** allow overlap but reject stale checkpoint commits, and ensure all downstream writes are replay-safe.

A global pipeline concurrency limit can serialize all tables but may reduce throughput. Per-source locking or compare-and-swap offers finer control, but needs careful failure recovery. The control-table compare-and-swap alone does not prevent duplicate landing files or make Silver writes idempotent.

## Acceptance tests

- Start two runs from the same prior checkpoint and different upper bounds. At most one stale checkpoint commit may succeed.
- Confirm a failed copy or Silver job never attempts to advance the checkpoint.
- Confirm a checkpoint update that matches zero or multiple rows fails visibly and emits an actionable incident.
- Confirm a conflict does not delete or silently overwrite landing data; recovery reconciles and replays safely.
- Confirm a null initial checkpoint follows the explicit initial-load branch.
- Record the old checkpoint, captured upper bound, run ID, update row count, and final checkpoint for every acceptance run.

## Current status

The exported ADF definition now sets pipeline concurrency to 1. This serializing setting is committed in the repository but has not been deployed or runtime-validated in Azure. The compare-and-swap SQL pattern remains a design reference and has not been executed against the SQL database or validated as an ADF Script expression. Do not mark optimistic checkpoint conflict detection as implemented until that mechanism is committed and tested.
