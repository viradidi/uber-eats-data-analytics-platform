# Legacy SQL Silver Job — Historical Recovery Record

## Current implementation

The original workspace that hosted Databricks job `121055824417886` was deleted. That workspace-scoped job definition cannot be exported from the deleted workspace.

**This is no longer an active deployment dependency.** The ADF SQL pipeline has been changed from the old `DatabricksJob` activity to a direct `DatabricksNotebook` activity:

`/Shared/UberEats/notebooks/Silver_SQL_Incremental`

The repository-owned replacement is implemented in:

- `notebooks/Silver_SQL_Incremental.py`
- `src/common/sql_silver.py`
- `sql/migrations/20261105_create_sql_silver_control.sql`
- `pipeline/sql_incremental pipeline.json`
- `deployment/databricks/import_workspace_assets.ps1`

The new pipeline path receives one exact deterministic CSV batch, checks the per-source metadata gate, validates the source contract, performs a version-aware Delta MERGE, reconciles the source keys, writes a run audit, and only then allows ADF to advance the SQL watermark checkpoint.

See [SQL Silver job replacement and source contract](SQL_SILVER_JOB_RECONSTRUCTION.md) for implementation behavior, source onboarding, and restored-environment acceptance.

## Historical recovery only

The original job export helper at `deployment/databricks/export_sql_silver_job.ps1` may be useful only if an independent copy of the old workspace/job becomes available, such as:

- A retained job settings JSON export.
- An exported SQL Silver notebook or script.
- A Git commit or external source repository used by the old job.
- An archived workspace backup or deployment package.

Do not run the old export procedure against a newly created workspace expecting job ID `121055824417886` to exist. A new workspace has its own workspace-scoped job IDs.

If a backup is recovered later, compare its source logic with the repository-owned replacement as historical reference material. Do not reintroduce the old Job ID into ADF without an explicit technical review and a reason to depart from the repository-owned notebook.

## Remaining acceptance work

The replacement implementation is source-controlled, but SQL source contracts are environment-specific and intentionally not fabricated.

Before enabling a source:

1. Apply `20261105_create_sql_silver_control.sql` to the control database.
2. Approve the exact fully qualified Silver target, business key, required fields, complete Spark type map, watermark semantics, and delete policy.
3. Populate and enable the corresponding `dbo.sql_silver_control` row only after approval.
4. Deploy the repository notebooks/modules to the restored Databricks workspace.
5. Run one bounded interval, replay the exact same interval, and prove stable target keys/values.
6. Force a Silver failure and verify that the ADF checkpoint remains unchanged.
7. Retain ADF/Databricks run IDs, source/landing/target counts, audit results, and reconciliation evidence.

The repository tests validate local parsing, batch validation, and ADF wiring; they do not execute a live Delta MERGE or restored Azure pipeline.
