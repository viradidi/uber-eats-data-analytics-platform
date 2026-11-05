# SQL Silver Job Reconstruction

## Decision and current state

The original Databricks workspace that contained SQL Silver job `121055824417886` has been deleted. The ADF pipeline still references that external job ID through `Job_Silver_copy1`, but the repository does not contain the job definition or its SQL Silver notebook/source.

Unless an archived workspace, exported job JSON, notebook export, Git commit, or other backup is recovered, the original job cannot be inspected or reproduced exactly. A fresh Databricks workspace should not be expected to contain the old workspace-scoped job ID.

**Required change:** replace the external `DatabricksJob` activity with a repository-owned SQL Silver notebook activity and deploy its source with the existing Databricks workspace import process. The old job export script is only a fallback if an independent backup becomes available.

## What can be retained

The committed ADF SQL extraction pattern provides a bounded, deterministic landing batch:

- One approved active row from `dbo.pipeline_control` is processed at a time.
- The source query uses the configured schema, table, watermark column, prior checkpoint, and captured maximum watermark.
- The copied rows are written as CSV under the ADLS folder associated with `table_name`.
- The output filename encodes the control ID and lower/upper watermark interval, using the pattern `ctl{control_id}_{lower}_{upper}.csv`.
- The checkpoint update depends on the Silver activity succeeding.

This is enough to define the new notebook's input boundary. It is **not** enough to infer the SQL target write mode, merge keys, schema rules, or delete policy.

## Why a generic append is not an acceptable replacement

The SQL extraction is incremental. Re-reading an existing interval or retrying after a partial Silver write can duplicate records if the destination simply appends. A Delta `MERGE` is not automatically safe either: it needs a verified business key, deterministic source version/tie-breaking rules, and an explicit update/insert/delete policy.

The current `pipeline_control` metadata contains `target_table`, but it does not define a Silver business key, source schema contract, write strategy, or replay policy. Do not invent those values from index candidates or table names.

## Required source contract before implementation

Complete and approve [the incremental source contract](INCREMENTAL_SOURCE_CONTRACT.md) for every SQL source that will be enabled. Use [the read-only source catalog inventory](../sql/diagnostics/inspect_source_contract_candidates.sql) against the source SQL database after exporting approved active control rows from the control database.

For every source, establish and record:

1. Exact source schema/table and source owner.
2. Exact Silver target catalog/schema/table.
3. Stable business key (single or composite) and uniqueness evidence.
4. Watermark type, precision, timezone, and insert/update behavior.
5. Whether rows can arrive late or share cursor values.
6. Delete/tombstone/CDC treatment.
7. Explicit target write strategy and deterministic version rule.
8. Required columns, types, nullability, and transformation rules.
9. Same-interval replay and reconciliation expectations.
10. A source-owner approval for the contract.

A primary/unique index provides candidate uniqueness evidence, not proof that its columns represent the correct business entity key.

## Repository-owned replacement design

The replacement should be implemented as a canonical Databricks SOURCE notebook under `notebooks/` (for example, `Silver_SQL_Incremental.py`) and imported by `deployment/databricks/import_workspace_assets.ps1`.

ADF should invoke the notebook directly and pass the exact metadata and batch identity needed for one control row. At minimum, the dispatch contract should include:

- `control_id`
- `schema_name` and `table_name`
- `target_table`
- Exact landing file path or exact deterministic filename (not the whole table folder)
- Lower and captured upper watermark values
- Approved business-key columns
- Approved write/replay strategy and source schema contract reference

If these fields are not available in `pipeline_control`, add them through a reviewed, backward-compatible migration or a separate SQL Silver metadata table. Keep source-specific decisions in metadata rather than hard-coding them in notebook branches.

### Required notebook stages

1. Validate all parameters and reject missing/unsafe identifiers or an unapproved write strategy.
2. Resolve and read only the exact landing batch file. Do not read every historical CSV in the table folder for an incremental run.
3. Apply an explicit source schema and deterministic column/type mapping; do not rely on uncontrolled schema inference for production-like ingestion.
4. Validate required columns, null keys, duplicate keys, cursor bounds, and supported record versions.
5. Reject or quarantine invalid rows and preserve failure details without masking the original exception.
6. Publish using the approved replay-safe strategy. For a key-based Delta merge, verify source key uniqueness before merge and define exact match/update conditions.
7. Reconcile expected source keys/counts to the destination and record a durable run/quality outcome.
8. Return success only after target publication and required reconciliation complete. ADF must advance the watermark checkpoint only after this success.

Do not implement hard-delete propagation unless the approved source contract provides a reliable delete signal.

## ADF integration and acceptance

The ADF change must remove the old `DatabricksJob` reference and replace it with a repository-owned notebook activity. Keep the checkpoint dependency after the new activity succeeds. Pass the precise file and source contract for the current `ForEach` item; do not derive a batch by scanning an entire Bronze folder.

Before activating SQL source rows in a restored environment:

- Confirm source and control SQL services are correctly separated.
- Apply and verify the control-table migrations.
- Deploy the replacement notebook and shared modules.
- Verify storage/catalog permissions with least-privilege identities.
- Test a normal bounded interval and capture source/landing/target counts.
- Reprocess the exact same lower/upper interval and prove no unintended target duplicates or data changes.
- Force a Silver failure and verify the checkpoint does not advance.
- Retain ADF/Databricks run IDs, file paths, counts, reconciliation results, and failure/retry evidence.

## Current release status

- Original external job export: **unavailable because the original workspace was deleted**, unless a separate backup is recovered.
- Repository-owned SQL Silver notebook: **not yet implemented**.
- ADF SQL activity replacement: **not yet implemented**.
- Source-specific target/key contracts: **pending source catalog discovery and owner approval**.
- Same-interval replay acceptance: **not run**.
- Existing local CI: validates the repository's current tests and contracts; it does not validate this not-yet-implemented SQL Silver replacement or a live Azure/Databricks environment.
