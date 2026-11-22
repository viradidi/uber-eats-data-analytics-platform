# Gold Atomic Publication and Recovery

## Problem addressed

The original Gold notebook overwrote five business tables one at a time. If a run failed after updating some tables but before updating the rest, consumers could read a mixture of outputs from different runs.

This feature replaces that publication pattern with immutable run-versioned snapshots and one active-publication pointer.

## Data objects

| Object | Responsibility |
|---|---|
| `gold_publication_state` | One row keyed by `gold_analytics`, holding the active publication ID |
| `gold_publication_history` | Append-only lifecycle events for staging, bootstrap, publishing, failures and warnings |
| `gold_order_360__versions` | Order-level snapshots partitioned logically by `_publication_id` |
| `gold_daily_order_performance__versions` | Daily snapshots partitioned logically by `_publication_id` |
| `gold_payment_method_performance__versions` | Payment-method snapshots partitioned logically by `_publication_id` |
| `gold_restaurant_feedback__versions` | Restaurant snapshots partitioned logically by `_publication_id` |
| `gold_driver_performance__versions` | Driver snapshots partitioned logically by `_publication_id` |
| Stable Gold view names | Consumer-facing views that filter their corresponding version table through the active publication pointer |

The five view names remain the same as the existing Gold table names, preserving the expected table interface for reports and downstream consumers.

## Publication protocol

1. Build and validate all five Gold outputs in memory. Existing source, grain, KPI and reconciliation checks still run before publication work begins.
2. Ensure the publication state/history tables and five version tables exist.
3. Capture the current active publication ID. This becomes the expected predecessor for this run.
4. Append every output to its own version table using the current Gold run ID as `_publication_id`.
5. Re-read the staged rows and verify the count for every output matches the validated DataFrame count.
6. Execute one Delta `MERGE` on `gold_publication_state`. The matched update is conditional on the active pointer still equalling the predecessor captured in step 3. The first publication can insert the pointer only when the expected predecessor is null.
7. Verify that the new publication ID is the active pointer. The five consumer views now resolve to the complete new version.
8. Append quality metrics, Gold run audit data and publication history as post-commit observability sidecars.

The pointer MERGE is the publication commit point. It is one atomic Delta transaction on one table. The five version tables are staged before that transaction, so their partial writes are not visible through stable consumer views.

## Failure and concurrency behavior

| Failure point | Consumer-visible outcome |
|---|---|
| Gold transformation or hard quality gate | Previous active publication remains visible |
| One or more version-table writes | Previous active publication remains visible; partial run ID is inactive |
| Staged row-count verification | Previous active publication remains visible |
| Concurrent publisher wins the pointer first | Stale run's compare-and-swap cannot activate its version |
| Pointer commit succeeds; quality/audit sidecar fails | Gold stays published; notebook records/prints a monitoring warning rather than claiming rollback |
| Process stops after pointer commit | New Gold publication remains active; audit/history may need manual review |

Inactive partial snapshots are retained for investigation until they meet the explicit [Gold publication retention policy](GOLD_PUBLICATION_RETENTION.md). The manual maintenance notebook defaults to dry-run, preserves in-flight/unknown/untracked data and never removes the active pointer or legacy baseline.

## First-deployment bootstrap

The first deployment must handle the five existing physical Gold tables. If all five exist as physical tables and there is no active pointer, the notebook copies them to a single baseline publication and commits that pointer before renaming any source table. It then renames each physical table to a unique `<name>__legacy_<suffix>` object and creates the stable view.

If no legacy Gold tables exist, empty version tables and stable views are created before the first publication is activated.

If the legacy set is partial, or stable views exist without a publication pointer, the notebook fails closed. It does not rename or discard the remaining objects. If a process stops after the baseline pointer is committed but before every view is created, the next run completes the remaining view replacements using the already-committed baseline. The new views are new Unity Catalog objects: review and explicitly re-apply the required consumer `SELECT` grants to each stable view after bootstrap; do not assume permissions on the renamed legacy tables automatically grant access to the views.

This is a one-time schema migration and requires Unity Catalog privileges to create Delta tables/views and rename existing tables. Run the first bootstrap with only one Gold publisher active; regular post-bootstrap publishes are protected by the pointer compare-and-swap, but the initial physical-table conversion is an administrative cutover rather than a concurrent migration protocol.

## Rollback procedure

Rollback uses a previously successful publication ID; it does not rewrite or copy business data.

1. Query `gold_publication_state` to record the currently active ID.
2. Select a prior event marked `PUBLISHED` or `LEGACY_BASELINE_IMPORTED` from `gold_publication_history`.
3. Confirm that every one of the five version tables contains rows for the target publication ID. For a valid empty output, confirm the table exists and that the history event records an output count of zero.
4. Use a compare-and-swap MERGE with the current active ID as the expected predecessor and the selected older ID as the new active ID. The same null-safe predecessor check prevents a stale rollback from overwriting a newer release.
5. Re-query the pointer and validate the stable views.

The SQL file `reports/GOLD_PUBLICATION_OPERATIONS.sql` includes a commented rollback template. It is intentionally not executed by the notebook or CI.

## Query consistency

Each stable view resolves through the same active-publication state row. A single query over the stable view therefore sees the publication pointer selected for that query. Separate SQL statements issued on opposite sides of a pointer switch may use different publication IDs. When a multi-query BI refresh requires one shared snapshot, read the active ID once and use it to filter the corresponding version tables throughout that refresh.

## Testing and deployment boundary

Local PySpark tests validate the publication ID helper, table naming, SQL construction, pointer compare-and-swap condition, schema definitions and injection guards. Repository tests verify that the canonical notebook stages and reconciles every dataset before calling the pointer MERGE and that the paired Jupyter export matches.

These tests do not execute Delta MERGE semantics, Unity Catalog table renames, view refresh, ADF orchestration, or Databricks permissions. A fresh Azure/Databricks acceptance run is required before claiming deployed atomic publication. The earlier Azure resources for this portfolio project were decommissioned, so this new release has not been applied to a live workspace.
