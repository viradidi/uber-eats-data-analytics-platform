# Restored Azure Acceptance Plan

## Purpose and status

This is the single execution plan for validating the restored Uber Eats platform across SQL ingestion, Silver processing, Gold publication, and reconciliation.

**Current decision: NOT YET RUNTIME-ACCEPTED.** Repository validation is green, and the original ingestion/Silver environment was historically executed successfully as confirmed by the project owner. The later SQL ADF hardening, Gold layer, master ADF orchestration, and their current deployment configuration still require a controlled live acceptance run.

Do not recreate Azure resources just to create portfolio screenshots. Use this plan when deliberately restoring a budgeted test environment.

## 1. Entry criteria — do not start the full master run until these pass

Start with the [restored Azure configuration inventory](RESTORED_AZURE_CONFIGURATION_INVENTORY.md). Before publishing ADF artifacts, run `deployment/azure/configure_restore_environment.ps1` from PowerShell 7 with the approved values from the restored environment. It updates historic SQL, ADLS, Key Vault, API, Databricks and factory bindings together; review the local `git diff` before publishing. The CSV branch has been moved from the retired Windows File Server/self-hosted Integration Runtime to the `bronze/csv_source` folder in ADLS.

### Repository and release identity

- [ ] Check out the reviewed repository commit and record its full SHA.
- [ ] Confirm the GitHub Actions workflow is green for that commit.
- [ ] Keep the CI result separate from ADF/Databricks runtime evidence.
- [ ] Confirm the exported ADF JSON, notebooks, SQL scripts, and source code match the reviewed commit.

### External SQL Silver job

- [ ] Export job `121055824417886` settings and every referenced SQL Silver notebook/script.
- [ ] Complete [the external SQL Silver job review](EXTERNAL_SQL_SILVER_JOB_REVIEW.md).
- [ ] Verify the table-to-input-folder and table-to-target mappings.
- [ ] Define the stable business/event key and destination write strategy for every enabled SQL source.
- [ ] Pass the same-bounded-interval-twice replay test in an isolated target before relying on SQL replay behavior.

If the job export is unavailable, do not claim SQL Silver replay safety. Do not enable an overlap strategy or run a destructive replay against a shared target.

### SQL source and control contracts

- [ ] Complete [the incremental source contract](INCREMENTAL_SOURCE_CONTRACT.md) for every active row in `dbo.pipeline_control`.
- [ ] Verify each source table, schema, watermark column, cursor type/precision/timezone, business key, initial-load position, late-arrival semantics, and delete strategy.
- [ ] Apply [the idempotent control-table bootstrap](../sql/migrations/20261104_create_ingestion_control_tables.sql) to create/verify the API and SQL ADF control tables and seed the four documented API endpoints.
- [ ] Add only reviewed SQL source-control rows; confirm the control table has unique, non-null `control_id` values and unique active source mappings.
- [ ] Confirm each active control row has an approved, non-null starting watermark. The current export does not implement a null-checkpoint initial-load branch; do not use an arbitrary universal sentinel.
- [ ] Review the configured schema/table/column identifiers against an approved allowlist and restrict control-table write access.
- [ ] Back up the SQL control database and review the migration plan before applying [the datetime2(7) checkpoint migration](../sql/migrations/20261104_widen_pipeline_checkpoint_to_datetime2_7.sql).
- [ ] Apply and verify the migration before deploying the updated ADF checkpoint dataset contract.

### Azure and Databricks deployment

- [ ] Restore the required Azure SQL source/control database, ADLS Gen2 storage, Data Factory, and Databricks workspace in a budgeted environment.
- [ ] Configure managed identities/service principals, Key Vault-backed secrets, least-privilege storage/SQL/catalog permissions, and network access.
- [ ] Deploy and verify the API source endpoint and its authentication setting.
- [ ] Authenticate the Databricks CLI to the restored workspace and run `deployment/databricks/import_workspace_assets.ps1`; it imports the canonical Silver/Gold notebooks, both dispatchers, and the shared `src` modules to the exact workspace paths used by ADF.
- [ ] Verify Databricks linked services, workspace URL, the SQL Silver job ID, cluster/runtime configuration, catalog/schema/table permissions, and library dependencies. API and CSV now invoke repository-owned notebooks directly; only the SQL Silver job remains external.
- [ ] Confirm `src` modules can be imported from `/Workspace/Shared/UberEats` and that the Silver/Gold notebooks receive the factory-level `bronze_root` parameter. The committed ADF linked services are aligned on 16.4 LTS and local Spark CI uses PySpark 3.5.2.
- [ ] Verify the restored ADLS source filesystem and permissions for `bronze/csv_source`.
- [ ] Import and publish the ADF datasets, linked services, child pipelines, SQL incremental pipeline, and master pipeline. Resolve all parameter, path, and dependency references before execution.

## 2. Pre-run input staging

After signing in with an identity allowed to write to the restored ADLS account, stage the two metadata JSON files expected by the ADF datasets:

```powershell
az login
az account set --subscription <RESTORED_SUBSCRIPTION_ID>
.\\deployment\\azure\\stage_bronze_metadata.ps1 -StorageAccountName <STORAGE_ACCOUNT_NAME>
```

Stage the three committed CSV source files into the same ADLS filesystem, in the source folder read by `DS_source_csv`:

```powershell
.\\deployment\\azure\\stage_csv_files.ps1 -StorageAccountName <STORAGE_ACCOUNT_NAME>
```

The script uploads and downloads each file to verify its SHA-256 hash. The CSV source no longer requires a Windows share or self-hosted Integration Runtime.

Authenticate and import Databricks workspace assets (from the repository root) after selecting the intended workspace and CLI profile:

```powershell
databricks auth login <WORKSPACE_HOST> --profile uber-eats-restore
.\\deployment\\databricks\\import_workspace_assets.ps1 -WorkspaceHost <WORKSPACE_HOST> -ProfileName uber-eats-restore
```

The import script verifies the notebook and module paths expected by the committed ADF definitions. It does not create SQL source tables or configure the external SQL Silver Job.

## 3. Pre-run smoke tests

Run these before processing business data:

| Component | Acceptance check | Evidence |
|---|---|---|
| API source | Authenticated request succeeds; missing/invalid key is rejected | Request result and API response |
| SQL source/control | Connection succeeds; control metadata and source schema preflight pass | Query output and control-row snapshot |
| ADLS | Intended service identity can read and write the configured landing path | Test object path and access result |
| Databricks | Job cluster starts; expected libraries and shared modules import | Job run URL and task output |
| Silver/Gold targets | Required catalog, schema, tables, and write permissions are present | Catalog/table checks |
| ADF | All references resolve; child pipelines and notebook path are valid | Publish/validation result |

Do not proceed if a smoke test requires a manual permission bypass or embedded credential.

## 4. Execute the ingestion and Silver acceptance

Use a controlled source data snapshot and record the run start time, commit SHA, ADF pipeline name, and run ID.

### API and CSV paths

1. Run the API ingestion child pipeline and confirm the expected endpoint files land in ADLS.
2. Run the CSV ingestion child pipeline and confirm the expected drivers, promotions, and zones files land.
3. Run the associated Silver transformations.
4. Verify schema, required fields, business/event keys, quality metrics, quarantine outcomes, and target reconciliation.
5. Confirm empty input or an invalid payload does not replace a populated target with an empty result.

### SQL incremental path

1. Record each active control row's control ID and lower checkpoint.
2. Run the SQL metadata preflight and source cursor-null validation.
3. Capture and retain the exact upper watermark for the batch.
4. Record the exact source query interval: `watermark > lower AND watermark <= upper`.
5. Run the ADF copy and retain landing file/object paths, counts, and schema evidence.
6. Run the exported SQL Silver job only after its source and destination behavior have passed the separate review.
7. Reconcile the exact source interval to landing and then to the Silver target using the approved business/event key and field-level checks. Use the templates in [SQL incremental-run acceptance queries](SQL_INCREMENTAL_RUN_ACCEPTANCE_QUERIES.md).
8. Repeat the same exact interval in the approved test target and verify that business-table results are unchanged and no unintended duplicates or missing keys appear.
9. Confirm the checkpoint advances to the captured upper bound only after the required Silver processing and validation succeed.

The checkpoint transaction protects the control-row compare-and-swap. It does not create a distributed transaction across SQL, ADLS, and Databricks. Inspect partial writes before every retry.

## 5. Execute the master pipeline and Gold publication

After the child paths have passed their individual checks:

1. Start `uber_eats_master` with a recorded pipeline run ID.
2. Verify API, CSV, and SQL child pipelines all reach `Succeeded`.
3. Verify `Publish_Gold_Analytics` starts only after all three child pipelines succeed.
4. Verify the notebook receives the parent ADF `pipeline_run_id`.
5. Verify the five business Gold outputs are created or updated in the intended catalog/schema:
   - `gold_order_360`
   - `gold_daily_order_performance`
   - `gold_payment_method_performance`
   - `gold_restaurant_feedback`
   - `gold_driver_performance`
6. Verify `gold_run_audit` records the same parent pipeline run ID, final status, output row counts, and timestamps.
7. Run the read-only business reporting SQL pack and reconcile its outputs to the Gold table grains and metric definitions in [Gold layer and reporting](GOLD_LAYER_AND_REPORTING.md).

Validate counts, distinct keys, nullability, domain constraints, relationships, and agreed aggregate tolerances. Do not accept a table merely because it exists or the notebook task reports success.

## 6. Recovery and controlled failure tests

Perform failure injection only in an isolated environment where the impact is understood and approved.

| Scenario | Required result |
|---|---|
| API/CSV/SQL copy failure | Downstream dependent work does not publish a success state |
| SQL Silver job failure after copy | SQL checkpoint remains at the prior value |
| Checkpoint compare-and-swap conflict | Transaction rolls back; the competing checkpoint is reread and reconciled |
| Gold failure | Master run fails; no successful Gold audit status is recorded |
| Reconciliation mismatch | Acceptance fails visibly; mismatch is retained for investigation |
| Safe rerun | Only the intended bounded scope is replayed; no unexplained target duplicates or missing keys appear |

After each recovery, use the [recovery and rerun runbook](RECOVERY_AND_RERUN_RUNBOOK.md). Never advance a checkpoint manually merely to make a pipeline green.

## 7. Evidence to retain

For the same acceptance cycle, retain:

- Repository commit SHA and successful CI workflow URL.
- ADF master and child pipeline run URLs/IDs, start/end times, and activity outcomes.
- SQL control-row state before/after, source tables, exact lower/upper bounds, and preflight results.
- ADLS landing paths and source/landing row counts.
- Databricks job/notebook run URLs/IDs, task output, source version, and target tables.
- Silver quality, quarantine, failure, and reconciliation results.
- Gold target table counts, distinct key counts, aggregate checks, reporting query outputs, and `gold_run_audit` row.
- Same-interval replay comparison and any controlled failure/recovery evidence.
- Any deviations, skipped tests, known limitations, and approvals.

Store evidence under the process in [Run Evidence and Retention](RUN_EVIDENCE_RETENTION.md). Redact secrets and personal data. Do not invent missing historical run IDs, row counts, screenshots, or success records.

## 8. Final go/no-go decision

The restored release is accepted only when all of the following are true:

- [ ] External SQL Silver job definition/source has been reviewed and replay semantics proven for each enabled source.
- [ ] Source-specific SQL contracts and initial checkpoint values are approved.
- [ ] Required SQL migration is applied and verified.
- [ ] All ADF references, identities, permissions, and Databricks paths resolve.
- [ ] API, CSV, and SQL ingestion complete successfully.
- [ ] Silver quality and reconciliation checks pass.
- [ ] Exact SQL interval replay passes with no unexplained business-target changes.
- [ ] Master ADF orchestration waits for all child branches before Gold publication.
- [ ] All five Gold outputs and the run-audit row reconcile.
- [ ] Required recovery scenarios behave as expected.
- [ ] Actual run evidence is retained and tied to the reviewed commit.

Until then, the project can accurately be described as repository-tested, with the original ingestion/Silver implementation historically executed by the project owner, and the newer SQL hardening/Gold/master-orchestration scope awaiting restored-environment acceptance.
