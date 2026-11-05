# Final Project Handoff

## Project status

**Status: original ingestion and Silver platform completed and historically executed end to end; Gold/reporting enhancements added afterward and awaiting a fresh Azure rerun. Cloud resources were decommissioned to control costs.**

This repository is a reviewable, Azure-oriented data engineering portfolio project. Its committed artifacts cover a FastAPI mock source, synthetic sample data, API/CSV/SQL ingestion patterns, exported Azure Data Factory resources, Databricks Silver notebook exports, reusable Gold transformations and reporting SQL, shared Spark utilities, data-quality checks, reconciliation utilities, CI validation, and recovery documentation.

The project owner confirms that the original API/CSV/SQL ingestion and Silver implementation ran successfully in its original Azure environment. Those cloud resources were later intentionally decommissioned to avoid ongoing costs. The Gold layer and master ADF pipeline are newer repository enhancements and have not yet been deployed and run in Azure. The local-mode PySpark suite does run in GitHub Actions and passed 121 tests in [run 41901084241](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/41901084241); that validates synthetic in-memory transformations, not workspace deployment, Delta writes, or end-to-end orchestration. The repository therefore distinguishes historical execution, current repository CI evidence, and pending cloud validation.

This is owner-reported execution history, not independent third-party certification. Repository artifacts can be inspected directly; retained historical runtime evidence should be linked where available.

## What is delivered

- Multi-source ingestion design, exported child ADF resource definitions, and a new master pipeline that gates Gold publication on success of all three ingestion paths.
- FastAPI mock source with API-key protection and source endpoints.
- Bounded SQL incremental extraction design, success-dependent checkpoint advancement, serialized pipeline runs, and a transactional checkpoint compare-and-swap guard that rolls back failed or non-unique updates.
- Active SQL control-row filtering (`is_active = 1`), pre-loop rejection of null/duplicate control IDs across the control table and duplicate active source mappings, and a fail-closed guard when no sources are enabled; SQL source-metadata preflight for schema/table/watermark identifiers and datetime-family cursor types; null-watermark rejection before extraction; and a datetime2(7) checkpoint migration to preserve cursor precision.
- Replay-safe API Silver preparation for timestamped cumulative endpoint snapshots: exact duplicates are collapsed, versioned entity records use the latest source update timestamp, and conflicting same-version records fail closed.
- Configurable Bronze roots, repository-owned shared Spark utilities, five Gold analytics outputs, and a read-only business reporting SQL pack.
- Data-quality checks, API contract tests, local PySpark transformation tests, static ADF/recovery contract tests, repository artifact integrity tests, and GitHub Actions validation.
- Quarantine, quality metrics, reconciliation helpers, failure-event logging, operational SQL reports, and rerun/recovery guidance represented in the repository.
- Architecture, source contracts, concurrency contract, limitations, and portfolio evidence documentation.
- Cell-level parity between six paired Databricks SOURCE and Jupyter notebook exports, with CI enforcing canonical `.py` deployment sources.
- Explicit source/control SQL linked-service separation and a read-only source database inventory for cursor and unique-key candidates.

## Documentation handoff

Start with the [README](../README.md), then use these documents for deeper review:

1. [Architecture overview](../architecture/architecture.md) and [architecture diagram](../architecture/uber-eats-end-to-end-architecture.svg)
2. [SQL incremental-load review](SQL_INCREMENTAL_LOAD_REVIEW.md)
3. [Incremental source contract](INCREMENTAL_SOURCE_CONTRACT.md)
4. [Checkpoint concurrency contract](CHECKPOINT_CONCURRENCY_CONTRACT.md)
5. [Notebook export consistency](NOTEBOOK_EXPORT_CONSISTENCY.md)
6. [Read-only SQL source catalog discovery](../sql/diagnostics/inspect_source_contract_candidates.sql)
5. [Operations and reliability](OPERATIONS_AND_RELIABILITY.md)
6. [Recovery and rerun runbook](RECOVERY_AND_RERUN_RUNBOOK.md)
7. [Gold layer and business reporting](GOLD_LAYER_AND_REPORTING.md)
8. [Run evidence and retention](RUN_EVIDENCE_RETENTION.md)
9. [Portfolio evidence pack](PORTFOLIO_EVIDENCE.md)
10. [Consolidated release-readiness review](RELEASE_READINESS_REVIEW.md)
11. [SQL Silver job reconstruction plan](SQL_SILVER_JOB_RECONSTRUCTION.md)
12. [Legacy external SQL Silver job review](EXTERNAL_SQL_SILVER_JOB_REVIEW.md)
13. [Restored Azure acceptance plan](RESTORED_AZURE_ACCEPTANCE_PLAN.md)
14. [Restored Azure configuration inventory](RESTORED_AZURE_CONFIGURATION_INVENTORY.md)
15. [Databricks workspace import script](../deployment/databricks/import_workspace_assets.ps1)
15. [Restored Azure configuration script](../deployment/azure/configure_restore_environment.ps1)
16. [ADF REST deployment script](../deployment/azure/deploy_adf_resources.ps1)
17. [Legacy SQL Silver job export helper](../deployment/databricks/export_sql_silver_job.ps1) (usable only if an archived copy of the original workspace/job becomes available)

## Validation and evidence boundaries

The project owner confirms successful end-to-end execution in the original environment. The repository also contains automated validation definitions and tests. These are different forms of evidence:

- **Historical cloud execution:** confirmed by the project owner; Azure resources are now decommissioned.
- **Repository validation:** inspect the workflow and tests, including local-mode PySpark execution and static ADF contract checks. These validate code and exported definitions, not Azure runtime services.
- **New Gold scope:** local Spark tests can validate transformations against synthetic in-memory inputs; only a later ADF/Databricks rerun can validate deployment, paths, identities, schemas, and actual Delta writes.
- **Independent verification:** do not imply an external audit, current live deployment, production SLA, or certification unless separately supported by evidence.

Where retained, preserve real pipeline/job run records, reconciliation outputs, test results, and commit references. Do not invent run IDs, metrics, row counts, screenshots, or outcomes. If a particular artifact was not retained, say “not retained” rather than estimating.

## Engineering limitations to explain

The project is a strong implementation and design portfolio, but reviewers should be able to discuss its boundaries:

- A bounded upper watermark limits an extraction window; it does not by itself solve late visibility, equal or non-monotonic cursors, or deletes.
- Source correctness depends on an explicit cursor contract and suitable overlap, replay, CDC, or reconciliation strategy where required.
- Pipeline concurrency set to one serializes runs of that pipeline; it does not coordinate unrelated pipelines or external writers.
- Compare-and-swap protects checkpoint advancement against stale updates, but does not alone make landing and Silver writes idempotent.
- The SQL ADF pipeline still references external Databricks job ID `121055824417886`, but the original workspace was deleted and the job definition/notebook is not present in this repository. A repository-owned SQL Silver replacement is required before a fresh workspace deployment; source-specific targets, keys, and replay semantics must be approved rather than inferred.
- Dynamic SQL identifiers must be validated against an allowlist and protected control metadata.
- Repository tests do not substitute for a current live Azure integration run.

Use the linked source contracts and runbooks when discussing mitigations or future extensions. Do not describe proposed enhancements as already implemented.

## Safe portfolio description

> Built an Azure-oriented multi-source data engineering platform integrating API, CSV, and SQL sources through Azure Data Factory, ADLS Gen2, and Databricks/PySpark. The original ingestion/Silver implementation ran successfully in its original Azure environment. Added a Gold analytics extension with order, daily performance, payment, driver, and restaurant-feedback datasets, local Spark tests, and a master ADF orchestration definition; these newer additions are awaiting a fresh Azure runtime validation. Cloud resources were decommissioned to manage costs.

Describe successful execution as project-owner-confirmed history. Reserve claims such as production-certified, independently audited, or guaranteed end-to-end idempotent for cases where specific acceptance criteria and supporting evidence establish them.

## No-cost posture and future operation

The source code, exported resource definitions, sample fixtures, tests, and documentation remain reviewable in GitHub. There is no requirement to recreate Azure resources just to keep the portfolio online or to produce screenshots.

If the project is redeployed in the future, use a budgeted environment, least-privilege identities, secret management, explicit source contracts, and the documented release gates. Run the PowerShell 7 restore-configuration script to update environment-bound linked services and endpoints, review the diff, run the ADF REST deployment script to publish the factory/linked services/datasets/pipelines, then run the Databricks workspace import script to deploy the Silver/Gold notebooks, dispatchers, and shared `src` modules; verify the master pipeline's child-pipeline references and notebook paths; run ADF/Databricks end to end; reconcile Gold table counts; and retain the resulting evidence before making new runtime or performance claims.
