# Incremental Source Contract

## Purpose

Complete this contract for each SQL source table before enabling incremental extraction for production-like use. The exported ADF pipeline uses a strict lower watermark bound and a captured upper bound; that pattern is correct only when the source's change semantics support it.

This is a design and deployment gate. It does not claim the current Azure pipeline has been changed or runtime-tested.

## Table contract

Copy this section once per source table and replace every placeholder.

| Field | Required decision |
|---|---|
| Source table | `<schema.table>` |
| Stable business key | `<single key or composite key>` |
| Watermark column | `<column>` |
| Watermark data type and precision | `<datetime2 precision / integer / other>` |
| Initial-load behavior | `<explicit full load / approved type-safe starting value>` |
| Does every relevant update change the watermark? | `<yes/no, with source evidence>` |
| Can multiple rows share a watermark value? | `<yes/no>` |
| Can late-arriving rows have a watermark <= the checkpoint? | `<yes/no/unknown>` |
| Delete capture | `<CDC / soft-delete column / separate feed / not captured>` |
| Expected change volume | `<typical and peak>` |
| Replay behavior | `<overwrite deterministic range / merge by key / deduplicate>` |
| Reconciliation rule | `<counts plus key/field checks>` |
| Owner / approval | `<responsible person or team>` |

Do not leave an unknown source behavior implicit. If it is unknown, record it as a limitation and do not claim reliable change capture for that case.

## Select the extraction strategy

### Strategy A — Monotonic, immutable watermark

Use the current `watermark_column > last_watermark_value AND watermark_column <= captured_max_watermark` pattern only if the source contract guarantees that new eligible rows always receive a watermark greater than the committed checkpoint and relevant changes cannot occur without moving that watermark.

A stable upper bound limits the current extraction window. It does not solve missed updates that retain an old watermark or late rows that arrive at/below the checkpoint.

### Strategy B — Mutable records with a reliable modified timestamp

If every relevant update changes a source-modified timestamp, consider a controlled overlap window and idempotent upsert into the target. Document the overlap duration, timezone/precision, merge key, tie-breaking rule, and reconciliation. The checkpoint should advance only to a successfully processed boundary. An overlap alone is insufficient without replay-safe writes.

### Strategy C — Timestamps are not sufficient

If source updates do not reliably advance the timestamp, or late visibility can occur behind the checkpoint, use a stronger change mechanism where available: database change tracking/CDC, a monotonic change sequence, or a composite cursor supported by the source. A composite cursor must use a stable tie-breaker and lexicographic boundary logic; simply adding a key to the selected columns does not solve the boundary problem.

### Deletes

A query over rows with a greater watermark cannot discover hard deletes unless the source exposes deletion events, tombstones, CDC, or a separate reconciliation mechanism. State explicitly whether deletes are out of scope.

## Control-table and SQL safety

The exported pipeline interpolates `table_name` and `watermark_column` from `pipeline_control` into SQL text. Before enabling a table:

- Restrict write access to the control table.
- Validate schema/table and column identifiers against an approved allowlist or metadata registry.
- Ensure the watermark value is serialized according to its declared SQL type and precision.
- Reject missing control fields rather than silently substituting a universal sentinel.
- Verify the configured business key and watermark column exist in the source.

Identifiers generally cannot be protected by ordinary value parameterization; use an allowlist for dynamically selected table/column names.

## Required recovery tests

For each table, define expected results and run these tests in an available test environment before calling the strategy runtime-verified:

1. Initial load with an empty or uninitialized control row.
2. No-change run (must not write an incorrect checkpoint).
3. Normal bounded extraction with expected lower/upper boundary rows.
4. Multiple source rows with the same watermark value.
5. Row inserted after the upper watermark was captured.
6. Late-arriving row with a watermark equal to or below the checkpoint, if the source can produce it.
7. Update to an existing business key.
8. Copy succeeds but downstream transformation fails; checkpoint must remain unchanged.
9. Rerun the same logical interval; target records and landing artifacts must follow the documented replay policy.
10. Source delete, if delete capture is in scope.
11. Invalid table/column metadata is rejected before dynamic SQL executes.
12. Reconciliation confirms expected keys, fields, and counts for the processed scope.

Record the run ID, test input, expected outcome, actual outcome, and evidence location. Repository-level static tests do not replace these source/runtime tests.

## Current project status

The committed ADF export has a bounded upper watermark and success dependencies from copy to Silver processing to checkpoint update. The current export does not explicitly handle null starting watermarks, does not show an allowlist guard for dynamic identifiers, and does not by itself guarantee capture of late/equal-watermark updates or replay-safe landing writes. Azure runtime validation is pending because the Azure for Students subscription is unavailable.
