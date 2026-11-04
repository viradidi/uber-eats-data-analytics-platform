# Project Release Gate

## Release decision

**Portfolio status: ORIGINAL INGESTION/SILVER SCOPE COMPLETED; GOLD EXTENSION AWAITS LIVE VALIDATION.** The project owner confirms that the original end-to-end ingestion and Silver implementation ran successfully in its Azure environment. Resources were subsequently decommissioned to control costs. The Gold notebook, master ADF pipeline, and business reporting SQL are newer additions that require a fresh Azure environment rerun. The local-mode PySpark suite has passed in GitHub Actions: 121 tests in [run 41899236348](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/41899236348).

The historical execution statement is owner-reported. It is not an independent audit, production certification, or proof that the environment is still running. New local PySpark tests validate Gold transformations against synthetic DataFrames; static tests validate the exported ADF dependency contract. Neither executes ADF or Databricks in Azure.

## Delivered scope

- FastAPI mock source API and synthetic domain data.
- Exported Azure Data Factory definitions for API, CSV, and SQL ingestion patterns.
- ADLS Gen2 landing integration and Databricks Silver notebook exports.
- Five Gold Delta output definitions, a run-audit record contract, a Gold notebook export, a read-only business reporting SQL pack, and a master ADF pipeline that gates Gold publication on all three ingestion pipelines succeeding.
- SQL bounded-watermark extraction, active-source filtering, pre-loop rejection of null/duplicate active control IDs and duplicate source mappings, serialized pipeline configuration, and transactional compare-and-swap checkpoint update. Source metadata validation is routed to the source SQL database while checkpoint writes use the separate control SQL database.
- Shared PySpark utilities, data-quality and reconciliation helpers, operational SQL reports, CI checks, and recovery documentation.

## Evidence and validation boundaries

| Area | What is present | Evidence boundary |
|---|---|---|
| Repository integrity | JSON/notebook exports, Python source, automated tests, GitHub Actions workflow | A specific CI run must be checked for its commit and conclusion |
| API behavior | FastAPI implementation and endpoint/security tests | Local tests do not prove an Azure-hosted API deployment |
| ADF orchestration | Exported pipeline, dataset, and linked-service definitions | Export inspection is not a current ADF runtime execution |
| Incremental SQL | Bounded extraction query, watermark control, preflight and source contracts | Correctness depends on source-specific cursor semantics and deployment safeguards |
| Databricks/Silver | Notebook exports, shared utilities, validation and reconciliation code | Original Silver execution is owner-reported historical status; a new environment must still be checked |
| Gold transformation logic | Shared PySpark module, Gold notebook exports, and local Spark unit tests | Local tests prove behavior for synthetic fixtures; they do not prove target catalog access or Delta publication |
| Gold orchestration/reporting | Master ADF JSON, static dependency tests, and SQL report pack | Static contracts do not prove ADF publication, Databricks workspace paths, or dashboard deployment |
| Historical end-to-end run | Project owner confirms successful end-to-end execution before resource teardown | Detailed run IDs, row counts, and scenario-specific logs should only be claimed where retained |
| Replay and recovery | Runbooks and static dependency tests | Do not claim universal exactly-once or replay safety without scenario-specific evidence |

## Deployment gates before reuse

The current consolidated decision and action order are recorded in [RELEASE_READINESS_REVIEW.md](RELEASE_READINESS_REVIEW.md). Use [EXTERNAL_SQL_SILVER_JOB_REVIEW.md](EXTERNAL_SQL_SILVER_JOB_REVIEW.md) for the external SQL job export/replay review and [RESTORED_AZURE_ACCEPTANCE_PLAN.md](RESTORED_AZURE_ACCEPTANCE_PLAN.md) for the restored-environment execution.

Before redeploying or enabling a source in a new environment:

1. Validate the source table, business key, cursor type/precision, timezone, initial-load behavior, late visibility, equal cursor values, and delete semantics.
2. Confirm only approved `is_active = 1` control rows are enabled; validate that `control_id` is non-null and unique across the full control table and that active `(schema_name, table_name, watermark_column)` mappings are unique. Enforce an approved allowlist for dynamic SQL identifiers and restrict control-table write access.
3. Confirm typed watermark serialization and null-checkpoint handling.
4. Inspect/export the Databricks SQL Silver job invoked by ADF job ID `121055824417886` (its definition/notebook is not source-controlled here); verify its input-file selection, target write mode and business keys, then replay the same bounded interval twice and reconcile landing and target results.
5. Import Gold_Analytics to /Shared/UberEats/Gold_Analytics, deploy src.gold so the notebook can import the transformation module, and verify the Databricks linked service and catalog permissions.
6. Run the master pipeline and verify all three child pipelines complete before Gold starts; reconcile the Gold output counts and gold_run_audit row.
7. Test copy failure, Silver failure, checkpoint conflict, reconciliation mismatch, and safe rerun in the target environment.
8. Configure identities, least-privilege permissions, and secret-backed connection settings.
9. Retain actual run IDs, commit SHAs, counts, reconciliation results, and test outputs for any claims made.

These are redeployment acceptance gates, not a request to recreate cloud resources solely for portfolio presentation.

## Portfolio claims

Safe description: **an Azure-oriented data engineering platform integrating API, CSV, and SQL sources through ADF, ADLS Gen2, and Databricks/PySpark. The original ingestion/Silver implementation ran successfully in its original environment. A newer Gold analytics extension adds order, daily KPI, payment, driver, and restaurant-feedback datasets with local Spark tests that pass in GitHub Actions, plus SQL reporting. The Gold deployment and master ADF orchestration await a fresh Azure rerun.**

Do not claim a currently deployed service, independent production certification, guaranteed exactly-once processing, or unmeasured performance outcomes.


## New Gold extension release gate

- [x] Define Gold table grains and metric semantics from the current Silver fields.
- [x] Add reusable PySpark transformations and local synthetic-fixture tests.
- [x] Add a master ADF definition with success dependencies for the API, CSV, and SQL child pipelines.
- [x] Add business reporting SQL and run-audit retention contract.
- [x] Run the local-mode PySpark suite in GitHub Actions; 121 tests passed in [run 41899236348](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/41899236348).
- [ ] Import the Gold notebook and make the src.gold module available in the target Databricks workspace.
- [ ] Publish and validate the master ADF pipeline in the restored environment.
- [ ] Run the end-to-end flow and reconcile actual Gold row counts and audit records.
- [ ] Retain actual ADF, Databricks, and reconciliation evidence under the run-evidence process.
