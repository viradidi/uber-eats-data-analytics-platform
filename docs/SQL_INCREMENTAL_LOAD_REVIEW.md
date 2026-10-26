# SQL Incremental Load — Configuration Review

## Scope

This review covers the exported Azure Data Factory definition at `pipeline/sql_incremental pipeline.json`. It is a configuration review, not a claim that the current Azure deployment has been executed or runtime-tested.

## Incremental window

The SQL copy query uses a lower boundary from `pipeline_control.last_watermark_value` and an upper boundary captured by `Get max watermark_copy1`:

```sql
watermark_column > last_watermark_value
AND watermark_column <= captured_max_watermark
```

The upper bound is important. If source rows arrive while the pipeline is running, the current extraction has a stable end boundary. Rows arriving after the max-watermark lookup remain beyond that boundary and should be eligible for a subsequent run, rather than being skipped because the checkpoint advanced to a later, uncopied value.

## Checkpoint ordering

The configured success dependency is:

1. Check whether rows exist beyond the stored watermark.
2. If rows exist, capture the source's maximum watermark.
3. Copy rows in the bounded interval to the landing location.
4. Run the Silver Databricks job.
5. Update `pipeline_control.last_watermark_value` to the captured maximum only after the Silver job succeeds.

This prevents the control watermark from advancing merely because extraction started or the copy completed while the required Silver job subsequently fails.

## Known correctness limits requiring a source-specific decision

The exported expressions do **not** yet handle these cases explicitly. Do not describe the pipeline as safe for these conditions until they are resolved and tested.

### Null or uninitialized watermark

The control row's `last_watermark_value` is interpolated directly into the SQL predicate. There is no explicit initial-load branch or null-safe default. A null value may produce an invalid or unintended predicate depending on how the ADF expression and SQL string are evaluated. **Operational requirement:** seed every control row with a valid watermark compatible with its column type, or implement a deliberate initial-load strategy before running an uninitialized row. Do not invent one universal sentinel for date, numeric, and string watermark columns.

### Late-arriving updates and equal watermark values

The strict lower bound (`>`) can miss a changed record if its watermark is unchanged or if the source assigns a watermark value at or below the committed checkpoint after that checkpoint has advanced. Rows sharing a timestamp are all included only if that timestamp is greater than the prior checkpoint; a newly visible row with the same watermark value as the checkpoint is excluded.

Choose a source-appropriate strategy before relying on this pipeline for mutable data, such as a reliable source-modified timestamp, a composite ordering key, or a controlled overlap window paired with idempotent merge/deduplication. An overlap window is not a drop-in fix: the destination must tolerate replay, and the checkpoint semantics must be defined. A timestamp alone is not a reliable change-data-capture mechanism when updates do not advance it.

### Dynamic SQL metadata

The pipeline concatenates `table_name` and `watermark_column` from `pipeline_control` into SQL text. Treat this control table as privileged configuration: validate table and column values against an allowlist and restrict who can modify control rows. The current export does not show an allowlist guard. Do not populate these fields from untrusted input.

### Landing-file replay and idempotency

The landing file name includes the current UTC timestamp. A retry can therefore create a second file for the same logical source range. The checkpoint dependency protects against advancing too early, but does not prevent duplicate landing files or guarantee idempotent Silver processing. Define a run/range identifier, replay policy, and downstream deduplication/merge contract before claiming end-to-end idempotency.

## Recovery implications

- If the copy fails, the watermark update is not reached; rerun from the stored watermark.
- If the Silver job fails, the watermark update is not reached; rerun the interval after resolving the failure.
- If the pipeline succeeds, the next run starts from the committed watermark, subject to the source-watermark limitations above.
- Replaying the same interval still requires the destination and Silver write strategy to tolerate repeated input. A checkpoint alone does not guarantee end-to-end idempotency.

## Remaining validation

Before describing this path as runtime-verified, validate in an available Azure environment:

- ADF validates the exported pipeline and all activity/dataset references.
- Control rows have valid, non-null starting watermarks or an explicitly implemented initial-load path.
- A successful run copies the expected interval and runs the Silver job.
- A forced Silver failure leaves the stored watermark unchanged.
- A subsequent retry processes the interval without duplicate target records or unintended duplicate-file effects.
- A row inserted after the captured upper boundary is processed by the next run.
- A row with the same watermark as the committed lower boundary is handled according to the chosen source-specific strategy.
- Late-arriving updates to existing business keys are captured according to the source change contract.
- Source/landing/Silver counts and key-level reconciliation match expected rules.

The user's Azure for Students subscription is currently disabled, so these runtime checks may need to wait until an appropriate environment is available. Repository-level configuration can still be reviewed and maintained in GitHub.
