# Incremental Source Contract

## 1. Purpose

Complete this contract for every SQL source table before enabling incremental extraction. The project's exported Azure Data Factory (ADF) pattern uses a strict lower watermark boundary and a captured upper boundary:

```sql
watermark_column > last_watermark_value
AND watermark_column <= captured_max_watermark
```

This pattern is appropriate only when the source's change semantics support it. A watermark is a progress cursor; it is not automatically a complete change-data-capture mechanism.

This document defines the source-onboarding requirements and acceptance criteria. It does not claim that a new Azure deployment has been created or runtime-tested.

## 2. Source-table contract

Copy and complete this table for each source table. Replace every placeholder with a verified source-specific decision.

| Field | Required decision |
|---|---|
| Source system and table | `<system.schema.table>` |
| Business purpose / owner | `<purpose and accountable owner>` |
| Stable business key | `<single key or composite key>` |
| Watermark column or change cursor | `<column / sequence / CDC position>` |
| Cursor data type and precision | `<datetime2 precision / integer / other>` |
| Timezone and timestamp semantics | `<UTC/local/unspecified; event time or modification time>` |
| Initial-load strategy | `<full load / approved type-safe starting position>` |
| Do inserts always receive a new cursor value? | `<yes/no; explain>` |
| Does every relevant update advance the cursor? | `<yes/no; explain>` |
| Can multiple rows share one cursor value? | `<yes/no; tie-breaker>` |
| Can a row become visible behind the checkpoint? | `<yes/no/unknown; explain>` |
| Delete capture | `<CDC / tombstone / soft delete / separate feed / not captured>` |
| Expected change volume and frequency | `<typical and peak>` |
| Destination write strategy | `<merge by key / replace deterministic range / other>` |
| Replay and deduplication rule | `<how repeated input is handled>` |
| Reconciliation rule | `<counts, keys, field checks, tolerances>` |
| Retention / source-history limits | `<retention window and implications>` |
| Approval | `<responsible person or team and date>` |

Do not leave unknown source behavior implicit. Record uncertainty as a limitation and do not claim reliable change capture for that case.

## 3. Choose the change-capture strategy

### Strategy A — Monotonic, immutable watermark

Use the current strict-lower-bound pattern only when the source guarantees that each newly eligible row has a cursor greater than the committed checkpoint, and relevant changes cannot occur without advancing that cursor.

A captured upper bound makes the current batch finite. It does not recover an update that retains an old timestamp or a row that becomes visible with a cursor at or below the checkpoint.

### Strategy B — Mutable records with a reliable modification timestamp

If every relevant update advances a reliable source-modified timestamp, a controlled overlap window may be appropriate:

1. Read from a position earlier than the committed checkpoint by the agreed overlap duration.
2. Bound the extraction with a captured upper position.
3. Deduplicate and merge into the destination using a stable business key.
4. Advance the checkpoint only after the required downstream work and validation succeed.

Document the overlap duration, timestamp precision and timezone, key semantics, tie-breaking behavior, destination merge rules, and reconciliation checks. An overlap is useful only when repeated records can be processed safely.

### Strategy C — Timestamp is not a reliable change cursor

If updates do not reliably advance the timestamp, or records can become visible behind the committed position, prefer a stronger source mechanism where available:

- Database change data capture (CDC).
- Database change tracking.
- A monotonic source change sequence.
- A composite cursor with deterministic tie-breaking and lexicographic boundary predicates.

A composite cursor must be supported by the source query and checkpoint design. Merely selecting an additional key column does not solve equal-timestamp boundary gaps.

### Deletes

A query selecting rows above a watermark cannot detect hard deletes unless the source exposes deletion events, tombstones, CDC, soft-delete attributes, or a separate reconciliation process. State whether deletes are captured, how they are applied downstream, or whether they are explicitly out of scope.

### API endpoint snapshot replay policy

The API ingestion pipeline writes timestamped files per endpoint, and the API Silver notebooks read the endpoint folder. Repeated runs can therefore expose the same source record in multiple files. The shared helper `src/common/incremental.py` now prepares these cumulative folders before Silver publication:

| Dataset | Business/event key | Replay/version rule |
|---|---|---|
| Payments | payment_id | Keep the greatest parseable updated_at per payment |
| Deliveries | delivery_id | Keep the greatest parseable updated_at per delivery |
| Ratings | rating_id | Keep the greatest parseable updated_at per rating |
| Order status | order_id + status_timestamp | Collapse exact duplicate events; reject conflicting payloads at the same event key |

For versioned entity records, the helper rejects different payloads with the same business key and the same updated_at instead of selecting an arbitrary winner. Null keys and missing or unparseable update timestamps also fail the batch. Exact replay copies are collapsed. The source update timestamp must still advance reliably for every meaningful change; the helper cannot recover a change that the source does not version.

The API Silver tables are still rebuilt from the accumulated landing snapshots after this deterministic preparation. This is replay-safe for repeated snapshots under the stated key/version contracts; it is not a claim of transactional multi-table publication or exactly-once delivery.

This API snapshot policy is separate from the SQL incremental ADF pipeline. The SQL pipeline still uses a strict lower watermark boundary. Its late/equal-watermark and delete limitations remain until each SQL source has a verified cursor contract and a source-appropriate overlap, CDC, tombstone, or reconciliation strategy.

## 4. Control-table and SQL safety

The exported pipeline interpolates `table_name` and `watermark_column` from `pipeline_control` into SQL text. Treat these values as privileged configuration.

Before enabling a source:

- Confirm that `pipeline_control.last_watermark_value` is deployed as `datetime2(7)`. Apply [the precision migration](../sql/migrations/20261104_widen_pipeline_checkpoint_to_datetime2_7.sql) if the existing column is the earlier `datetime` type; the migration preserves nullability and checkpoint values.
- Confirm that the ADF SQL metadata preflight resolves the configured schema, table, and watermark column, and that the cursor uses a supported datetime type. The current export accepts `datetime`, `datetime2`, and `smalldatetime` only.
- Keep schema/table/column names within the simple identifier contract accepted by the preflight; control-table write access remains privileged.
- Restrict write access to the control table.
- Review the preflight as a syntax/existence guard, not a replacement for source-specific authorization or an approved allowlist.
- Verify the configured table, watermark, and business key exist in the source.
- Serialize watermark values using the declared SQL type, precision, and timezone semantics.
- Reject missing or invalid control fields; do not silently substitute a universal sentinel.
- Use typed SQL parameters for values where supported. Value parameters do not generally parameterize identifiers, so identifiers still require allowlist validation.
- Define a safe initial-load path for each cursor type.

## 5. Checkpoint and destination contract

Document and verify these behaviors together:

| Concern | Required behavior |
|---|---|
| Checkpoint ownership | Identify the process allowed to advance the source cursor |
| Checkpoint commit point | Advance only after the required copy, transformation, and validation steps succeed |
| Concurrent updates | Detect stale checkpoint state; define behavior when another writer changes it |
| Partial writes | Identify how incomplete landing or target writes are detected and repaired |
| Replay | Reprocessing the same logical interval must not create unintended duplicates or corrupt target state |
| Deduplication | Define deterministic winner selection when keys or cursor values repeat |
| Atomicity boundary | State which operations are transactional and which require reconciliation |
| Observability | Record source interval, run ID, row counts, checkpoint before/after, and failure details |

The ADF pipeline's concurrency setting and compare-and-swap-style update help protect the checkpoint update under the conditions they cover. They do not create a universal lock against external writers, and they do not make storage or Spark writes transactional or idempotent by themselves.

## 6. Required acceptance tests

For each source table, define expected results and execute these tests in an available environment before describing the source strategy as runtime-verified:

1. Initial load with an empty or uninitialized control row.
2. No-change run; confirm that the checkpoint does not move incorrectly.
3. Normal bounded extraction, including records at the lower and upper boundaries.
4. Multiple source rows sharing the same watermark value.
5. A row inserted after the upper watermark has been captured.
6. A late-visible row whose cursor is equal to or below the committed checkpoint, if possible for the source.
7. An update to an existing business key.
8. A downstream transformation failure after successful copy; confirm the checkpoint remains unchanged.
9. Replay of the same logical interval; verify landing and destination behavior.
10. A stale checkpoint / concurrent update conflict.
11. Invalid table or column metadata; confirm rejection before dynamic SQL executes.
12. Source-to-landing and source-to-target reconciliation for the processed scope.
13. Delete capture and downstream application, if deletes are in scope.

For every test, preserve the run ID, source scenario, expected result, actual result, checkpoint before and after, reconciliation output, and evidence location. Mark a test as not run if no runtime evidence is available. Repository static checks do not substitute for source-specific runtime tests.

## 7. Source onboarding decision

A source is ready for incremental onboarding only when:

- The business key and change cursor are defined.
- Initial-load and null-checkpoint behavior are explicit.
- Timestamp precision, timezone, and tie behavior are understood.
- Late-arriving changes and deletes are addressed or explicitly excluded.
- Dynamic identifiers are validated.
- Destination writes are replay-safe.
- Checkpoint ownership and commit conditions are defined.
- Reconciliation and operational recovery are documented.
- Required acceptance tests have passed and evidence is retained.

If one of these conditions is unknown, record the limitation, identify an owner, and do not present the source as having proven reliable incremental capture.

## 8. Current project implementation and status

The committed ADF SQL incremental pipeline export includes a captured upper watermark, a strict lower watermark predicate, success dependencies from copy to the Silver job to checkpoint update, and pipeline concurrency set to one. It reads only `is_active = 1` control rows, rejects null or duplicate `control_id` values across the control table, rejects duplicate active `(schema_name, table_name, watermark_column)` mappings, and fails when no active SQL source is configured. The checkpoint compare-and-swap runs inside a SQL transaction with `XACT_ABORT`, `TRY/CATCH`, rollback on failure, and error 51001 when the affected-row count is not exactly one. The pipeline performs a SQL catalog preflight for `schema_name`, `table_name`, and `watermark_column` before constructing source queries, honors the configured schema, and uses bracket-quoted identifiers after validation. A pre-extraction data check also fails the run if any source row has a null watermark value. The exported control dataset describes the checkpoint as `datetime2(7)` and a preserving migration script is provided for the physical SQL control table.

The export still does not explicitly show a null-safe initial-load branch for a null control checkpoint. The metadata preflight checks that configured source objects exist, restricts identifier syntax, and validates datetime-family watermark types before queries execute; a separate data check rejects source rows with null watermark values. These controls are not a substitute for control-table access restrictions or a source-specific allowlist. The strict lower bound does not automatically capture late/equal-watermark changes. The pipeline has no configured overlap/CDC/delete policy or source business-key metadata for a replay-safe SQL Silver merge. It invokes external Databricks job ID `121055824417886`, but the corresponding SQL Silver job definition/notebook is not present in this repository; its target write mode, merge keys, and repeat-interval behavior must be inspected and tested before SQL replay safety can be claimed. The landing path does not by itself prove replay-safe writes. The physical datetime2(7) migration is committed but must be applied and verified in the target SQL control database.

The project owner confirms that the completed platform was successfully executed end to end. The Azure resources used during that execution were later decommissioned to control costs. The current repository can be reviewed and its available automated checks can be run, but there is no currently deployed Azure environment in this project to use for a new live acceptance cycle. Do not confuse decommissioning with an incomplete project, or describe historical owner-confirmed execution as a new independent runtime test.

## 9. Related documentation

- [SQL Incremental Load Engineering Review](SQL_INCREMENTAL_LOAD_REVIEW.md)
- [Checkpoint Concurrency Contract](CHECKPOINT_CONCURRENCY_CONTRACT.md)
- [Recovery and Rerun Runbook](RECOVERY_AND_RERUN_RUNBOOK.md)
- [Reconciliation Audit Queries](RECONCILIATION_AUDIT_QUERIES.sql)
