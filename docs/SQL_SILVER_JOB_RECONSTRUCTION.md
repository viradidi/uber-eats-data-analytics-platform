# SQL Silver Job Replacement and Source Contract

## Decision

The original Databricks workspace containing SQL Silver job `121055824417886` was deleted. Its workspace-scoped job definition cannot be exported from that deleted workspace. Rather than preserving an unusable Job ID, the ADF pipeline now calls a repository-owned Databricks notebook directly.

The replacement is committed as:

- `notebooks/Silver_SQL_Incremental.py` — Databricks SOURCE notebook entry point.
- `src/common/sql_silver.py` — reusable metadata parsing, typed batch validation, version-aware Delta MERGE, reconciliation, and run-audit helpers.
- `sql/migrations/20261105_create_sql_silver_control.sql` — per-source SQL Silver contract table in the control database.
- `deployment/databricks/import_workspace_assets.ps1` — imports the notebook and shared module into the target workspace for deployment or future redeployment.
- `pipeline/sql_incremental pipeline.json` — invokes the notebook directly after the bounded SQL copy and advances the watermark only after notebook success.

The historical job source itself is not reconstructed byte-for-byte. The replacement is an explicit, repository-owned implementation with a documented contract.

## Preserved ADF extraction boundary

The SQL ADF branch retains its bounded extraction pattern:

- It processes approved active rows from `dbo.pipeline_control`.
- It queries the configured source schema/table and watermark column.
- It captures an upper watermark and copies rows where the cursor is strictly greater than the prior checkpoint and at or below the captured maximum.
- It writes one deterministic CSV file under the ADLS Bronze folder named after the source table.
- The batch filename encodes the control ID and lower/upper watermark interval: `ctl{control_id}_{lower}_{upper}.csv`.
- The SQL checkpoint update depends on successful completion of `Job_Silver_copy1`, now a direct `DatabricksNotebook` activity.

The notebook receives the exact CSV filename, never a table-folder wildcard. Its file path is constructed from the configured `bronze_root`, source table folder, and batch filename.

## Per-source metadata contract

The new `dbo.sql_silver_control` table is created in the **control SQL database**. It stores one configuration row per SQL `control_id`:

| Field | Purpose |
|---|---|
| `control_id` | Foreign key to `dbo.pipeline_control.control_id` |
| `is_enabled` | Explicit approval gate; defaults to disabled |
| `write_strategy` | Currently constrained to `DELTA_MERGE` |
| `business_key_columns_json` | JSON array of stable single/composite key column names |
| `required_columns_json` | JSON array of fields that must be populated; business keys and watermark are required automatically |
| `column_types_json` | JSON object mapping every source CSV column to an approved Spark type |

The migration does not invent or enable source contracts. The ADF pipeline checks that every active `pipeline_control` row has an enabled and structurally populated SQL Silver configuration before the per-source copy loop begins.

Before setting `is_enabled = 1`, complete [the incremental source contract](INCREMENTAL_SOURCE_CONTRACT.md) and obtain source-owner approval for the target table, key, cursor, type mapping, update semantics, and deletes. The target table stored in `dbo.pipeline_control.target_table` must be a fully qualified `catalog.schema.table` name. The notebook rejects unsafe or incomplete metadata.

Example shape only — replace every placeholder with values verified from the actual source; this is not an executable source mapping:

```sql
UPDATE dbo.sql_silver_control
SET
    business_key_columns_json = N'["<approved_key_column>"]',
    required_columns_json = N'["<approved_key_column>","<watermark_column>"]',
    column_types_json = N'{"<approved_key_column>":"string","<watermark_column>":"timestamp"}',
    write_strategy = 'DELTA_MERGE',
    is_enabled = 1,
    updated_at = SYSUTCDATETIME()
WHERE control_id = <approved_control_id>;
```

The JSON type map must include every column present in the ADF CSV header, not only the key and watermark. Supported Spark types are explicitly allowlisted in `src/common/sql_silver.py`, including primitive numeric/string/boolean/date/timestamp/binary types and bounded `decimal(p,s)` types.

## Notebook behavior and safety

For every non-empty bounded SQL batch, the repository-owned notebook:

1. Validates control IDs, source identifiers, target table, JSON contract, and the exact `.csv` filename.
2. Reads only that exact batch file with header parsing and schema inference disabled.
3. Applies the source-approved Spark column types; it fails on header drift or non-empty values that cannot be cast.
4. Requires the business keys and watermark to be non-null; validates configured required fields.
5. Verifies every row is within the declared exclusive lower/inclusive upper watermark interval.
6. Rejects duplicate business keys in the batch; it never selects an arbitrary winner.
7. Creates the Delta target on the first approved batch or applies a Delta MERGE by approved business key.
8. Updates a matched target row only when the incoming watermark is strictly newer. Exact same-version replay is a no-op; conflicting payloads at the same key/version fail. Older versions cannot overwrite newer target rows.
9. Reconciles that every source key exists in the target after publication.
10. Appends a run outcome to `catalog.schema.sql_incremental_run_audit`.

If publishing or the required audit write fails, the notebook raises an error and ADF does not advance the checkpoint. A replay of the same interval is protected by the same key/version merge. The MERGE is not a global transaction with ADLS and SQL; the checkpoint remains an ADF dependency gate.

## Explicit boundaries

- The target is a current-state Delta table. Hard-delete propagation is not implemented because it requires a verified delete signal or a separate reconciliation design.
- The current ADF source cursor is a strict timestamp watermark. It can miss equal/older late-visible rows unless the source contract proves that cannot happen or a future approved overlap/CDC strategy is introduced.
- The notebook rejects duplicate business keys within a batch. Sources that legitimately emit multiple events per key need an event-grain key or another explicit source contract; do not weaken the check without defining the model.
- The type map is source-specific and must match the exact SQL CSV output. Schema drift fails closed rather than silently widening or inferring types.
- The helper's validation tests run with local PySpark. The project owner now confirms that the deployed SQL Silver path, Delta processing, and end-to-end SQL/ADF flow executed successfully. Detailed notebook run exports have not yet been attached to the repository.

## Restore and deploy

1. Back up the control database.
2. Apply `20261104_create_ingestion_control_tables.sql` if the ADF control tables do not already exist.
3. Apply `20261104_widen_pipeline_checkpoint_to_datetime2_7.sql` and verify the checkpoint precision.
4. Apply `20261105_create_sql_silver_control.sql` to the control database.
5. Add approved SQL rows to `dbo.pipeline_control`, but keep them inactive until their source and target contract is reviewed.
6. Restore ADF, Databricks, and ADLS; configure identities and least-privilege permissions.
7. Run `deployment/databricks/import_workspace_assets.ps1` and verify `/Shared/UberEats/notebooks/Silver_SQL_Incremental` and `/Shared/UberEats/src/common/sql_silver.py`.
8. Populate and approve one `dbo.sql_silver_control` row per source; enable only the source being accepted.
9. Publish ADF and run one bounded interval in an isolated target.
10. Re-run the exact same lower/upper interval; verify stable key counts/values, no target duplicates, and unchanged checkpoint on a forced Silver failure.
11. Retain ADF/Databricks run IDs, exact batch file, target counts, audit records, and reconciliation evidence.

## Current implementation status

- [x] Replace the deleted external Job ID with a repository-owned ADF notebook activity.
- [x] Add the metadata-driven SQL Silver SOURCE notebook and shared Spark helper.
- [x] Add a disabled-by-default per-source contract table and ADF preflight gate.
- [x] Update the workspace importer and local contract tests.
- [x] Discover and approve the source target/key/type contracts used by the deployed active SQL source configuration (project-owner confirmed; environment-specific values are not published here).
- [x] Apply the control database migration and populate the approved metadata in the deployed environment (project-owner confirmed).
- [x] Execute the same-interval replay validation in the deployed environment (project-owner confirmed).
- [ ] Run an isolated failure-injection test after SQL copy and verify the checkpoint does not advance when the Silver notebook or audit step fails; this is an additional resilience test, not a blocker to the owner-confirmed successful end-to-end run.

The code replacement is present in GitHub and the project owner confirms that it was deployed and included in the successful end-to-end Azure run. Exact ADF/Databricks run identifiers and detailed SQL replay outputs remain to be attached to the acceptance record.
