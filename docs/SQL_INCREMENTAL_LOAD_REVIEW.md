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

1. **Read and validate active control configuration** — `Lookup_SQL_Control` reads only rows from `dbo.pipeline_control` where `is_active = 1`. `Validate active SQL control metadata` rejects null/duplicate `control_id` values across the full control table and duplicate active `(schema_name, table_name, watermark_column)` mappings; `Active SQL control metadata is unique` fails with `SQL_DUPLICATE_ACTIVE_CONTROL_METADATA` before the loop. `Require active SQL control rows` checks the length of the filtered lookup result and fails with `SQL_NO_ACTIVE_CONTROL_ROWS` if it is empty, preventing the master pipeline from treating an empty SQL branch as a successful refresh before Gold publication.
2. **Validate source metadata** — `Validate SQL source metadata` checks that the configured `schema_name`, `table_name`, and `watermark_column` exist, use a supported datetime type, and satisfy the simple identifier contract.
3. **Reject null source cursors** — `Validate SQL watermark nulls` fails the run if any source row has a null value in the configured watermark column.
4. **Check for new data** — `Check any new data` counts source rows whose watermark is greater than the stored lower watermark.
5. **Capture the upper bound** — when the check finds rows, `Get max watermark_copy1` retrieves the source's current maximum watermark.
6. **Copy the bounded interval** — `adls data migration_copy1` copies rows above the stored watermark and at or below the captured maximum to the configured ADLS destination.
7. **Run Silver processing** — `Job_Silver_copy1` calls the repository-owned `/Shared/UberEats/notebooks/Silver_SQL_Incremental` notebook after the copy succeeds. It reads the exact deterministic CSV batch, validates its enabled key/type contract, performs a version-aware Delta MERGE, reconciles source keys, and writes a run audit.
8. **Commit the checkpoint** — `update watermark_copy1` updates `pipeline_control.last_watermark_value` to the captured maximum only after the repository-owned Silver notebook and audit write succeed.

The checkpoint update also compares the current control-row watermark with the prior value read by the run. It now executes inside a SQL transaction with `XACT_ABORT` and `TRY/CATCH`; if the update affects anything other than one row, it raises SQL error 51001 and rolls the transaction back. Errors raised during the update also reach the catch block and trigger rollback. The exported pipeline declares `concurrency: 1`, which serializes runs of this pipeline when deployed and honored by ADF.

The source count, maximum-watermark, and copy queries honor `schema_name` and bracket-quote identifiers after the preflight. The preflight compares metadata values as escaped SQL string literals and only accepts simple identifier characters before those values are used as query identifiers.

This ordering prevents the checkpoint from advancing merely because extraction started or the copy succeeded while the required Silver job subsequently failed. The compare-and-swap predicate also detects a stale control-row value at commit time.

### 3.1 Separate source and control databases

The restore script supports a source SQL database that is different from the control SQL database. The ADF resource references preserve two connection boundaries:

- `Lookup_SQL_Control`, duplicate/active-control checks, and the checkpoint compare-and-swap update use `DS_sql_control` / `ls_sql_control`.
- Source catalog preflight, source watermark-null checks, row counts, maximum-watermark lookup, and source copy use `ls_sql_database` through the source datasets.

The source catalog preflight uses `ds_sql_water`, which is bound to `ls_sql_database`, rather than the control dataset. The checkpoint update uses `ls_sql_control`, not the source linked service. CI enforces this separation. This matters when restored source and control databases have different names or are hosted on different SQL servers; a query against the control database's `sys.schemas` cannot validate a table that exists only in the source database.


These mechanisms have limits: a pipeline-level concurrency setting is not a per-source lock against other pipelines or external writers, and checkpoint protection does not automatically make landing-file writes or Silver transformations idempotent.

## 4. Important correctness boundaries

The exported expressions do not explicitly resolve every source-specific edge case. These must be decided before enabling a source table in a production-like environment.

### 4.1 Control checkpoint precision

The exported control dataset previously described `last_watermark_value` as SQL `datetime` with scale 3. A source `datetime2(7)` cursor can contain precision beyond that representation. Rounding or truncating the checkpoint can cause strict lower-bound comparisons to skip or repeat rows.

The dataset contract now declares `datetime2(7)`, and the repository includes [a fail-closed migration](../sql/migrations/20261104_widen_pipeline_checkpoint_to_datetime2_7.sql). For a new restore, first apply the [ADF control-table bootstrap](../sql/migrations/20261104_create_ingestion_control_tables.sql), which creates `dbo.pipeline_control` with `datetime2(7)` when absent; then apply the widening migration, which safely no-ops when precision is already correct. Apply and verify that migration in the SQL control database before deploying the updated dataset/pipeline export. It preserves the column's existing nullability and does not reset checkpoint values. The migration has not been executed against the decommissioned environment.

The pipeline metadata preflight accepts `datetime`, `datetime2`, and `smalldatetime` source watermark columns. It does not accept date, string, numeric, or datetimeoffset cursor fields. A type match does not prove that a source timestamp is monotonic or that updates advance it. A separate pre-extraction check fails the run when any source row has a `NULL` watermark, because such rows cannot satisfy the incremental comparison.

### 4.2 Null or uninitialized watermark

The lower watermark is interpolated into the SQL expressions. The export does not show an explicit initial-load branch or null-safe default.

A null comparison in SQL does not behave like an ordinary lower-bound comparison. The new source-data guard rejects rows with a `NULL` watermark before extraction, but the control table's own `last_watermark_value` still requires a valid starting watermark. The export does not implement a null-safe initial-load branch. Each control row therefore needs a valid starting watermark of the correct source-column type until that branch is deliberately implemented. Do not use one universal sentinel across datetime, numeric, and string watermark types.

### 4.3 Late-arriving rows and equal watermark values

The strict lower bound (`>`) can miss a row that becomes visible after the checkpoint has advanced if that row's watermark is equal to or less than the committed value.

For example, if the checkpoint is `2026-01-10 12:00:00`, a newly visible row carrying that exact timestamp will not satisfy a strict greater-than predicate.

For mutable sources, choose an explicit strategy, such as:

- A reliable source-modified timestamp that advances for every relevant update.
- A composite cursor with deterministic tie-breaking and correct lexicographic boundary logic.
- CDC or database change tracking.
- A controlled overlap window combined with replay-safe destination merges and deduplication.

An overlap window is not a complete solution on its own. The destination must tolerate replay, and the checkpoint and reconciliation rules must be defined. A timestamp is not a reliable change-capture mechanism if updates do not advance it.

### 4.4 Deletes

A query that selects rows above a watermark cannot detect hard deletes unless the source exposes deletion events, tombstones, CDC, a soft-delete attribute, or a separate reconciliation mechanism. Define whether deletes are captured or explicitly out of scope for each source.

### 4.5 Dynamic SQL identifiers

The exported expressions still construct SQL text from control-table metadata, so these fields remain privileged configuration. The new preflight checks that the exact schema, table, and watermark column exist in SQL Server catalog metadata, restricts identifiers to letters/digits/underscores with a letter/underscore first character, and only then permits the count/max/copy queries. Those queries use bracket-quoted identifiers and the configured schema. This is a guard against malformed or injected identifier text, not a replacement for restricting control-table write access or for a deployment-specific allowlist. Do not populate control metadata from untrusted input.

### 4.6 Watermark typing and SQL construction

The current ADF expressions construct SQL strings using the stored and captured watermark values. Correctness depends on the source column type, precision, formatting, quoting, and the way ADF serializes the lookup output.

Validate these details for every source type. Prefer typed parameters or a stored procedure for values where supported. Identifiers still require allowlist validation because ordinary SQL value parameters do not parameterize table or column names.

### 4.7 Landing-file replay and destination idempotency

The landing filename is derived from the control ID and lower/upper watermark values and ends in `.csv`. The `DelimitedTextWriteSettings.fileExtension` is also set to `.csv` for consistency. Because the output dataset supplies an explicit `fileName` through `p_file_name`, Azure Data Factory uses that dataset filename and ignores the sink `fileExtension` setting ([Microsoft documentation](https://learn.microsoft.com/en-us/azure/data-factory/format-delimited-text)). This provides a deterministic identity for the same logical extraction interval and avoids assigning a fresh timestamp-based filename on every attempt.

A deterministic filename does not prove that the ADLS sink safely overwrites an existing object, nor does it guarantee that downstream Silver processing is idempotent. Confirm the sink's write/overwrite behavior and define the downstream merge, deduplication, or replacement strategy before claiming end-to-end idempotency.

### 4.8 Repository-owned SQL Silver and replay boundary

The `Job_Silver_copy1` activity is now a direct Databricks Notebook call to `/Shared/UberEats/notebooks/Silver_SQL_Incremental`; it passes the exact deterministic filename, bounded watermarks, target table, and approved source contract fields. The notebook uses `dbo.sql_silver_control` metadata and the shared helper `src/common/sql_silver.py`. It rejects duplicate input keys and invalid casts, then MERGEs by the approved business key and updates matched rows only when the incoming watermark is newer. Same-key/same-version payload conflicts fail; exact replay is a no-op. A durable audit record is appended after reconciliation.

These are code-level protections and local contract checks, not a live Delta acceptance result. Before enabling each source, approve its exact target, business key, column types, cursor semantics, and delete policy. Spark timestamps preserve microseconds, so the notebook explicitly rejects non-zero sub-microsecond source timestamps rather than silently truncating them. Then run the same captured lower/upper interval twice in restored Databricks and verify the business table remains stable, and prove a failed notebook leaves the SQL checkpoint unchanged. Do not claim complete change capture for late/equal cursors or hard deletes without a source-specific strategy.

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

- An active-source control filter (`is_active = 1`), a pre-loop guard that rejects null/duplicate control IDs and duplicate active source mappings, and a separate failure when no SQL control rows are enabled.

- A source-metadata preflight that verifies the configured schema/table/column, rejects unsupported watermark types and unsafe identifier characters, and runs before any source count/copy query.
- A source-data check that fails before extraction if any record has a null watermark value.
- Schema-aware, bracket-quoted source queries.
- A strict lower watermark and captured upper watermark for bounded extraction.
- A dependency from the copy activity to the Databricks Silver job.
- A dependency from successful Silver processing to checkpoint update.
- A transactional compare-and-swap checkpoint update that rolls back when the affected-row count is not exactly one and raises SQL error 51001.
- Pipeline concurrency set to one.
- A deterministic `.csv` landing filename derived from the control ID and watermark interval, with the delimited sink extension configured to `.csv` as well.

The control dataset now declares `last_watermark_value` as `datetime2(7)`. A preserving migration script widens the physical control column from datetime-family types to `datetime2(7)` without resetting its value or changing nullability. The migration must be run in the target SQL environment before deployment; it has not been run against the decommissioned environment.

The export still does not provide a null-watermark initial-load branch. Its strict lower bound does not capture late/equal-watermark changes automatically, and the deterministic landing filename does not prove replay-safe sink behavior. The deleted job ID `121055824417886` is no longer referenced by the ADF pipeline; the repository-owned SQL Silver notebook now implements a metadata-gated version-aware Delta MERGE, while source-specific target/key/type configuration and live replay remain pending. No per-source lookback/CDC/delete strategy is activated because the exported SQL control metadata and repository do not establish the business keys and downstream replay semantics needed to make overlap safe.

The project was successfully executed historically according to the project owner. Its Azure resources were later decommissioned to control costs, so a new Azure runtime acceptance cycle is not currently available from the retained environment. Repository inspection and static validation remain possible; they should not be described as a new live Azure test.

## 9. Engineering conclusion

The pipeline demonstrates a useful incremental-load pattern: bounded extraction, downstream-success-dependent checkpoint advancement, concurrency-aware checkpoint updates, and documented recovery considerations.

Its correctness depends on the source contract and the destination's replay behavior. The most important interview distinction is that **a watermark is a progress cursor, not a complete change-data-capture solution**. Reliable incremental processing also requires explicit treatment of late changes, deletes, ties, initialization, concurrency, idempotency, and reconciliation.
