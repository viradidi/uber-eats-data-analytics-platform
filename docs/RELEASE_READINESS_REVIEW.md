# Uber Eats — Consolidated Release Readiness Review

**Review date:** 2026-11-05  
**Repository:** [viradidi/uber-eats-data-analytics-platform](https://github.com/viradidi/uber-eats-data-analytics-platform)  
**Reviewed repository commit for CI evidence:** `c6e0c2cbe47dc3b16826653bf8d3e024963f7f99`  
**CI evidence:** [GitHub Actions run 41983203349 — 131 tests passed](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/41983203349)

## 1. Release decision

**Status: repository checks passed; cloud release pending.**

**Repository-side status: READY FOR CONTROLLED HANDOFF. Restored Azure release status: NOT YET ACCEPTED.**

The original API/CSV/SQL ingestion and Silver implementation were successfully executed end to end in the original Azure environment, as confirmed by the project owner. Those resources were later decommissioned to control costs. The repository now contains later SQL ADF hardening, Gold transformations/reporting, and master ADF orchestration changes. GitHub Actions passed the 121-test suite on the reviewed commit, including local-mode PySpark tests, ADF/static contracts, notebook portability checks, and repository integrity checks.

That CI result is not a live ADF, SQL Server, ADLS, or Databricks execution. The newer SQL changes and Gold/master pipeline still need a restored Azure acceptance run. No current cloud deployment is implied.

## 2. What is verified in the repository

| Area | Current repository evidence | Status |
|---|---|---|
| API source | FastAPI implementation, API authentication and endpoint tests | Repository-tested |
| API incremental replay | Shared PySpark helper and Silver notebook exports; exact replays collapse, versioned entities retain latest source version, conflicting versions fail closed | Local Spark tests passed |
| CSV ingestion/Silver | Exported ADF definitions, notebook exports, validation and reconciliation logic | Static/repository reviewed; original execution owner-confirmed |
| SQL ADF extraction | Active control filtering, duplicate/null metadata guards, source-database catalog preflight, null cursor guard, bounded window, deterministic landing identity, concurrency=1, transactional checkpoint compare-and-swap; source and control SQL endpoints are explicitly separated | Static contract tests passed, including split-database routing; new deployment not runtime-tested |
| SQL Silver destination behavior | Repository-owned notebook reads the exact batch, validates per-source schema/key metadata, performs a version-aware Delta MERGE, reconciles keys, and appends a run audit; source contracts and live replay remain pending | **Implemented; source onboarding/runtime acceptance open** |
| Gold transformation logic | Shared `src/gold` code, five output contracts, local PySpark fixtures/tests, Gold notebook export, business reporting SQL | Local Spark tests passed; workspace publication not proven |
| Master orchestration | Master ADF export waits for API, CSV, and SQL child pipelines before Gold publication | Static dependency checks passed; ADF execution pending |
| Notebook export consistency | Six paired Databricks SOURCE `.py` and Jupyter `.ipynb` exports match cell-for-cell; CI enforces parity and importer deploys canonical `.py` files | Repository-tested |
| SQL source discovery | Read-only source-database catalog inventory reports cursor type/precision and primary/unique-index key candidates; optional table scans are disabled by default | Repository artifact added; execution awaits restored SQL source access |
| Documentation and recovery | Source contracts, preflight SQL, runbooks, reconciliation queries, evidence retention, release gate | Repository reviewed |
| CI | Current verified run records 131 tests passing, including SQL Silver replacement/import checks | Passed |

The verified 131-test run includes regression coverage for the repository-owned SQL Silver notebook, its metadata gate, source-contract documentation, and workspace import/read-back paths. It remains repository evidence only; no CI result proves a live Delta MERGE, Azure Data Factory execution, or restored cloud permissions.

## 3. Open gates — priority order

A restore-specific configuration inventory is now available at [RESTORED_AZURE_CONFIGURATION_INVENTORY.md](RESTORED_AZURE_CONFIGURATION_INVENTORY.md). An idempotent [ADF control-table bootstrap migration](../sql/migrations/20261104_create_ingestion_control_tables.sql) now creates/verifies the two control tables and seeds only the four API endpoints documented in the repository; it deliberately leaves SQL source control rows and watermarks to be approved from real source contracts. It records historic endpoints, identities, paths, and job IDs that must be confirmed or replaced in the new environment.

### P0 — Restore environment-bound integrations

The current exports retain historic SQL, ADLS, Key Vault, API, Databricks workspace, and factory references. The CSV pipeline previously read from a Windows share through a self-hosted Integration Runtime. The source now reads from the ADLS `bronze/csv_source` folder. Its source dataset and copy read settings were updated, the retired File Server/Integration Runtime exports were removed, and a script now uploads and hash-verifies the three committed CSV files. The restored environment now only needs ADLS permissions and the staging script for this branch.

The Silver notebook Bronze-root defaults have been corrected to match the ADLS linked service, and the personal workspace path in the CSV dispatcher has been replaced by a configurable relative notebook root. A regression test now checks these contracts. Repeatable PowerShell scripts now stage the API/CSV metadata JSON and the three CSV source fixtures into ADLS, verifying remote file hashes. A Databricks workspace import script now imports the canonical notebooks and shared Python modules to the ADF-linked paths.

The ADF linked-service cluster defaults and local PySpark tests are aligned on Databricks Runtime 16.4 LTS / Spark 3.5.2. The SQL Silver notebook is now source-controlled and imported with the other notebooks; its runtime compatibility, catalog permissions, and Delta writes still need validation in the restored workspace. The PowerShell 7 restore-configuration script updates SQL, ADLS, Databricks, Key Vault, API and factory values together without writing secret values.

### P0 — Configure and accept repository-owned SQL Silver

The deleted-workspace Job ID `121055824417886` has been removed from the active ADF pipeline. `Job_Silver_copy1` now invokes `/Shared/UberEats/notebooks/Silver_SQL_Incremental` as a direct `DatabricksNotebook` activity. The workspace importer deploys this notebook and `src/common/sql_silver.py`.

The new `dbo.sql_silver_control` migration stores each source's business key, required fields, and complete Spark type map. It defaults configurations to disabled. ADF fails before the copy loop when any active SQL source lacks an enabled and populated config.

Required before enabling SQL sources:
- Apply `20261105_create_sql_silver_control.sql` to the control database.
- Complete and approve each source's target table, stable business key, cursor semantics, required fields, and complete `column_types_json`.
- Verify the source contract against source SQL catalog metadata; candidate unique indexes are not automatic business-key approval.
- Import the Databricks notebook/shared helper and verify ADLS/catalog permissions.
- Run the same exact captured lower/upper interval twice in an isolated target; reconcile target keys/values and confirm a forced Silver failure does not advance the checkpoint.

See [SQL Silver job replacement and source contract](SQL_SILVER_JOB_RECONSTRUCTION.md). The new notebook has not yet been executed in a restored Databricks workspace.

### P1 — Complete source-specific SQL contracts

The generic source-contract template is present, but each active SQL control row needs a verified contract for business key, cursor type/precision/timezone, initial watermark, equal/late-arriving changes, deletes, destination merge behavior, and reconciliation. The current ADF export requires a valid non-null starting checkpoint; it does not implement a null-checkpoint initial-load branch.

Use the read-only [source catalog discovery script](../sql/diagnostics/inspect_source_contract_candidates.sql) against the source database to inventory cursor metadata and unique-index candidates, then complete [Incremental Source Contract](INCREMENTAL_SOURCE_CONTRACT.md) for every enabled source and approve business keys and control-table values.

### P1 — Prepare the restored environment

Back up the SQL control database, apply/verify the ADF control-table bootstrap if needed, and apply/verify the `datetime2(7)` migration. Restore the required Azure SQL, ADLS Gen2, ADF, and Databricks resources; configure identities, secrets, network access, and least-privilege permissions. Run `deployment/azure/configure_restore_environment.ps1` with approved endpoints, review the local diff, run `deployment/azure/deploy_adf_resources.ps1` to publish the factory/linked services/datasets/pipelines, and run `deployment/databricks/import_workspace_assets.ps1` to import the notebook set and make `src.gold` and shared utilities available under `/Shared/UberEats/src`.

### P2 — Run one consolidated acceptance cycle

Execute API, CSV, and SQL ingestion; validate Silver quality and reconciliation; prove SQL same-interval replay; run the master pipeline; verify Gold publication and `gold_run_audit`; and test controlled failure/recovery scenarios in an isolated environment.

Use [Restored Azure Acceptance Plan](RESTORED_AZURE_ACCEPTANCE_PLAN.md) as the execution record, [Restored Azure Configuration Inventory](RESTORED_AZURE_CONFIGURATION_INVENTORY.md) to resolve historic references, and [Run Evidence and Retention](RUN_EVIDENCE_RETENTION.md) to preserve actual run evidence.

## 4. Known technical limitations to retain

These are not silently “fixed” by CI and should remain explicit until a source-specific design addresses them:

- Strict SQL lower watermark `>` can miss equal/older late-visible rows.
- The SQL source query does not capture hard deletes without CDC, tombstones, soft-delete flags, or reconciliation.
- SQL control rows need valid typed starting watermarks; null initial-checkpoint handling is not implemented.
- Pipeline concurrency=1 serializes this ADF pipeline only; it is not a global lock against external writers.
- Checkpoint compare-and-swap protects the SQL control-row update, not a distributed transaction spanning SQL, ADLS, and Databricks.
- Deterministic landing filenames do not establish sink overwrite or Silver target replay semantics.
- Gold tests use synthetic data and do not prove catalog access, Delta publication, or a live ADF run.
- The original historical execution is project-owner-confirmed; retained historical runtime evidence should only be claimed where it exists.

## 5. Safe portfolio statement

> Built an Azure-oriented, multi-source data engineering platform integrating API, CSV, and SQL ingestion through Azure Data Factory, ADLS Gen2, and Databricks/PySpark. The original ingestion/Silver implementation ran successfully in its original Azure environment. Added SQL incremental-load hardening and a Gold analytics extension with order, daily performance, payment, driver, and restaurant-feedback datasets, local Spark tests, business reporting SQL, and master ADF orchestration. The repository validation suite passes; the newer SQL hardening and Gold/master orchestration await a restored Azure acceptance run.

Do not claim the current project is deployed, independently audited, production-certified, or guaranteed exactly-once.

## 6. Review completion criteria

Repository-side review is complete when the committed artifacts and evidence remain consistent, the SQL Silver notebook and metadata gate are covered by passing CI, and source-specific configuration tasks are clearly recorded. Cloud release is complete only after every restored-environment go/no-go item in the acceptance plan has passed with actual evidence.
