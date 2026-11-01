# SQL Incremental Load — Engineering Review

## 1. Scope

This review examines the exported Azure Data Factory pipeline at `pipeline/sql_incremental pipeline.json`. It explains the implemented configuration, the reasoning behind its bounded watermark window, its checkpoint dependency chain, and the correctness conditions that must be considered for each source table.

The project owner confirms that the end-to-end project was successfully executed before its Azure resources were decommissioned to control costs. This document is a review of the committed pipeline definition; it does not imply that the Azure deployment is currently available for a new runtime test.

## 2. How the incremental window works

The copy query uses a lower boundary from `pipeline_control.last_watermark_value` and an upper boundary captured by the `Get max watermark_copy1` lookup:

```sql
watermark_column > last_watermark_value
AND watermark_column <= captured_max_watermark
```

The lower boundary represents the previously committed position. The upper boundary is captured for the current extraction window.

Capturing an upper bound matters because source data may continue to arrive while a pipeline is running. The current batch is limited to the captured maximum; records that become eligible after that point can be picked up by a later run, provided their watermark is greater than the committed checkpoint and the source's change semantics satisfy the incremental contract.

The upper bound therefore makes the extraction window finite and repeatable. It does not, by itself, guarantee that every insert, update, or delete will be captured.

## 3. Configured activity and checkpoint sequence

The exported pipeline defines this dependency chain inside the per-control-row processing flow:

1. **Read control configuration** — `Lookup_SQL_Control` reads rows from `pipeline_control`.
2. **Check for new data** — `Check any new data` counts source rows whose watermark is greater than the stored lower watermark.
3. **Capture the upper bound** — when the check finds rows, `Get max watermark_copy1` retrieves the source's current maximum watermark.
4. **Copy the bounded interval** — `adls data migration_copy1` copies rows above the stored watermark and at or below the captured maximum to the configured ADLS destination.
5. **Run Silver processing** — `Job_Silver_copy1` starts the configured Databricks job after the copy succeeds.
6. **Commit the checkpoint** — `update watermark_copy1` updates `pipeline_control.last_watermark_value` to the captured maximum only after the Silver job succeeds.

The checkpoint update also compares the current control-row watermark with the prior value read by the run. It raises SQL error 51001 if the update affects anything other than one row. The exported pipeline declares `concurrency: 1`, which serializes runs of this pipeline when deployed and honored by ADF.

This ordering prevents the checkpoint from advancing merely because extraction started or the copy succeeded while the required Silver job subsequently failed. The compare-and-swap predicate also detects a stale control-row value at commit time.

These mechanisms have limits: a pipeline-level concurrency setting is not a per-source lock against other pipelines or external writers, and checkpoint protection does not automatically make landing-file writes or Silver transformations idempotent.

## 4. Important correctness boundaries

The exported expressions do not explicitly resolve every source-specific edge case. These must be decided before enabling a source table in a production-like environment.

### 4.1 Null or uninitialized watermark

The lower watermark is interpolated into the SQL expressions. The export does not show an explicit initial-load branch or null-safe default.

A null comparison in SQL does not behave like an ordinary lower-bound comparison. Each control row therefore needs either a valid starting watermark of the correct source-column type or a deliberately implemented initial-load path. Do not use one universal sentinel across datetime, numeric, and string watermark types.

### 4.2 Late-arriving rows and equal watermark values

The strict lower bound (`>`) can miss a row that becomes visible after the checkpoint has advanced if that row's watermark is equal to or less than the committed value.

For example, if the checkpoint is `2026-01-10 12:00:00`, a newly visible row carrying that exact timestamp will not satisfy a strict greater-than predicate.

For mutable sources, choose an explicit strategy, such as:

- A reliable source-modified timestamp that advances for every relevant update.
- A composite cursor with deterministic tie-breaking and correct lexicographic boundary logic.
- CDC or database change tracking.
- A controlled overlap window combined with replay-safe destination merges and deduplication.

An overlap window is not a complete solution on its own. The destination must tolerate replay, and the checkpoint and reconciliation rules must be defined. A timestamp is not a reliable change-capture mechanism if updates do not advance it.

### 4.3 Deletes

A query that selects rows above a watermark cannot detect hard deletes unless the source exposes deletion events, tombstones, CDC, a soft-delete attribute, or a separate reconciliation mechanism. Define whether deletes are captured or explicitly out of scope for each source.

### 4.4 Dynamic SQL identifiers

The exported expressions concatenate `table_name` and `watermark_column` from `pipeline_control` into SQL text.

Treat these fields as privileged configuration. Validate schema, table, and column identifiers against an approved allowlist or metadata registry, restrict control-table write access, and reject unexpected values before query construction. The current export does not show an allowlist guard. Do not populate these identifiers from untrusted input.

### 4.5 Watermark typing and SQL construction

The current ADF expressions construct SQL strings using the stored and captured watermark values. Correctness depends on the source column type, precision, formatting, quoting, and the way ADF serializes the lookup output.

Validate these details for every source type. Prefer typed parameters or a stored procedure for values where supported. Identifiers still require allowlist validation because ordinary SQL value parameters do not parameterize table or column names.

### 4.6 Landing-file replay and destination idempotency

The landing filename is derived from the control ID and lower/upper watermark values. This provides a deterministic identity for the same logical extraction interval and avoids assigning a fresh timestamp-based filename on every attempt.

A deterministic filename does not prove that the ADLS sink safely overwrites an existing object, nor does it guarantee that downstream Silver processing is idempotent. Confirm the sink's write/overwrite behavior and define the downstream merge, deduplication, or replacement strategy before claiming end-to-end idempotency.

## 5. Failure and recovery behavior

Based on the exported success dependencies:

| Failure point | Expected checkpoint behavior | Recovery consideration |
|---|---|---|
| Control lookup or change-presence check fails | No checkpoint advancement | Investigate configuration or source connectivity |
| Bounded copy fails | Checkpoint update is not reached | Reconcile partial landing output before retry |
| Databricks Silver job fails | Checkpoint update is not reached | Inspect partial Silver writes and rerun safely |
| Checkpoint compare-and-swap fails | Update throws error 51001 | Re-read control state and reconcile before replay |
| Entire chain succeeds | Checkpoint is set to captured upper watermark | Next run begins from committed state, subject to source contract |

A failed downstream activity leaving the checkpoint unchanged is useful, but it does not guarantee that partial files or target writes were rolled back. Recovery must inspect those outputs and apply the documented replay policy.

See the [Recovery and Rerun Runbook](RECOVERY_AND_RERUN_RUNBOOK.md) and [Checkpoint Concurrency Contract](CHECKPOINT_CONCURRENCY_CONTRACT.md).

## 6. Source onboarding gate

Before enabling a new table, complete the [Incremental Source Contract](INCREMENTAL_SOURCE_CONTRACT.md). At minimum, document:

- Stable business key and watermark column.
- Watermark type, precision, and initial-load strategy.
- Whether every relevant update advances the watermark.
- Whether multiple rows can share a watermark value.
- Late-arrival and delete behavior.
- Expected volume and extraction frequency.
- Replay-safe landing and target-write semantics.
- Reconciliation rules and an accountable owner.

Do not enable a table solely because the pipeline JSON parses or the source returns rows.

## 7. Acceptance tests for a future deployment

When an Azure test environment is available, capture expected and actual results for:

1. Initial load with an empty or uninitialized control row.
2. A no-change run.
3. A normal bounded extraction, including lower- and upper-boundary records.
4. Multiple source rows sharing the same watermark.
5. A row inserted after the upper watermark has been captured.
6. A late-arriving row with a watermark equal to or below the committed checkpoint, if the source can produce one.
7. An update to an existing business key.
8. A failed copy and a failed Silver job; neither should advance the checkpoint.
9. A repeated run for the same logical interval, including landing-file and target-write behavior.
10. A checkpoint conflict caused by a stale prior value.
11. Invalid table or column metadata rejected before dynamic SQL executes.
12. Source-to-landing and source-to-Silver reconciliation, including key and field checks.
13. Delete capture, if deletes are in scope.

For each test, preserve the run ID, input scenario, expected outcome, actual outcome, checkpoint before/after, and evidence location.

## 8. Current review status

The committed pipeline definition contains:

- A strict lower watermark and captured upper watermark for bounded extraction.
- A dependency from the copy activity to the Databricks Silver job.
- A dependency from successful Silver processing to checkpoint update.
- A compare-and-swap-style checkpoint predicate with a visible SQL error on an unexpected update row count.
- Pipeline concurrency set to one.
- A deterministic landing filename derived from the control ID and watermark interval.

The export does not explicitly provide a null-watermark initial-load branch or dynamic-identifier allowlist. The strict lower bound does not capture late/equal-watermark changes automatically, and the deterministic filename does not prove replay-safe sink behavior.

The project was successfully executed historically according to the project owner. Its Azure resources were later decommissioned to control costs, so a new Azure runtime acceptance cycle is not currently available from the retained environment. Repository inspection and static validation remain possible; they should not be described as a new live Azure test.

## 9. Engineering conclusion

The pipeline demonstrates a useful incremental-load pattern: bounded extraction, downstream-success-dependent checkpoint advancement, concurrency-aware checkpoint updates, and documented recovery considerations.

Its correctness depends on the source contract and the destination's replay behavior. The most important interview distinction is that **a watermark is a progress cursor, not a complete change-data-capture solution**. Reliable incremental processing also requires explicit treatment of late changes, deletes, ties, initialization, concurrency, idempotency, and reconciliation.
