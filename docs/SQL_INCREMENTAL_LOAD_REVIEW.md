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

## Recovery implications

- If the copy fails, the watermark update is not reached; rerun from the stored watermark.
- If the Silver job fails, the watermark update is not reached; rerun the interval after resolving the failure.
- If the pipeline succeeds, the next run starts from the committed watermark.
- Replaying the same interval still requires the destination and Silver write strategy to tolerate repeated input. A checkpoint alone does not guarantee end-to-end idempotency.

## Remaining validation

Before describing this path as runtime-verified, validate in an available Azure environment:

- ADF validates the exported pipeline and all activity/dataset references.
- A successful run copies the expected interval and runs the Silver job.
- A forced Silver failure leaves the stored watermark unchanged.
- A subsequent retry processes the interval without duplicate target records.
- A row inserted after the captured upper boundary is processed by the next run.
- Source/landing/Silver counts and key-level reconciliation match expected rules.

The user's Azure for Students subscription is currently disabled, so these runtime checks may need to wait until an appropriate environment is available. Repository-level configuration can still be reviewed and maintained in GitHub.
