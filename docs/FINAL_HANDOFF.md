# Final Project Handoff

## Project status

**Status: deployed to Azure and end-to-end operation verified — project-owner confirmed.**

The project owner confirms that the completed API/CSV/SQL platform, repository-owned SQL Silver path, Gold analytics layer, and master ADF orchestration have now been deployed and exercised end to end. The accepted flow includes Bronze/Silver processing, incremental replay validation, Gold publication, and reconciliation. The original Azure environment was previously decommissioned to control costs; the platform has since been successfully deployed and validated again.

The latest repository validation is [GitHub Actions run 41984330196 — 135 tests passed](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/42021275338) on commit `034a19d5d62a94bb75e510f03e214f59590bf54a`. CI validates repository code and synthetic fixtures, while the successful Azure deployment and end-to-end runtime outcome are separately project-owner-confirmed.

The exact Azure deployment date, ADF/Databricks run IDs, row counts, and sanitized runtime exports have not yet been attached to this repository. See the [Azure deployment and acceptance record](DEPLOYMENT_ACCEPTANCE_RECORD.md). This is an evidence-retention gap, not a pending deployment gate.

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

Keep the evidence types distinct:

- **Repository validation:** the latest GitHub Actions run passed 135 tests. This validates the committed code, static resource contracts, and local/synthetic test scenarios.
- **Azure runtime acceptance:** the project owner confirms successful deployment and end-to-end execution across API, CSV, and SQL ingestion, Bronze/Silver processing, incremental replay, Gold publication, and reconciliation.
- **Retained runtime artifacts:** exact ADF/Databricks run IDs, detailed logs, timestamps, row counts, screenshots, and reconciliation exports have not yet been added to this repository. Do not invent them; attach sanitized originals when available.
- **Independent verification:** do not imply an external audit, production certification, continuous availability, or a production SLA unless separately supported.

See [Azure deployment and acceptance record](DEPLOYMENT_ACCEPTANCE_RECORD.md) and [Run Evidence and Retention](RUN_EVIDENCE_RETENTION.md) for the current evidence inventory and retention process.

## Engineering limitations to explain

The project is a strong implementation and design portfolio, but reviewers should be able to discuss its boundaries:

- A bounded upper watermark limits an extraction window; it does not by itself solve late visibility, equal or non-monotonic cursors, or deletes.
- Source correctness depends on an explicit cursor contract and suitable overlap, replay, CDC, or reconciliation strategy where required.
- Pipeline concurrency set to one serializes runs of that pipeline; it does not coordinate unrelated pipelines or external writers.
- Compare-and-swap protects checkpoint advancement against stale updates, but does not alone make landing and Silver writes idempotent.
- The deleted external Databricks job reference has been replaced with a repository-owned `Silver_SQL_Incremental` notebook activity. The project owner confirms the configured SQL Silver path was included in the deployed end-to-end run. The strict watermark, late-arrival, hard-delete, and null-checkpoint limitations documented in the source contract remain relevant.
- Dynamic SQL identifiers must be validated against an allowlist and protected control metadata.
- Repository tests do not substitute for a current live Azure integration run.

Use the linked source contracts and runbooks when discussing mitigations or future extensions. Do not describe proposed enhancements as already implemented.

## Safe portfolio description

> Deployed and verified an Azure-based multi-source data engineering platform integrating API, CSV, and SQL ingestion through Azure Data Factory, ADLS Gen2, and Databricks/PySpark. Implemented Bronze, Silver, and Gold processing, incremental SQL ingestion with checkpoint controls and replay validation, data-quality and reconciliation checks, operational audit records, and master-pipeline orchestration. The project owner confirms successful end-to-end operation after deployment; the repository validation suite also passed 135 tests.

Use measured row counts, performance improvements, run durations, or cost figures only when supported by retained runtime outputs. Do not claim independent audit, production certification, or a production SLA.

## Evidence preservation and future operation

The deployed end-to-end flow is complete according to the project owner. Preserve the corresponding ADF/Databricks run history, sanitized logs, reconciliation outputs, and deployment metadata when available. The repository includes an [Azure deployment and acceptance record](DEPLOYMENT_ACCEPTANCE_RECORD.md) with the accepted scope and the runtime fields still to be populated.

For future redeployment, use a budgeted environment, least-privilege identities, secret management, explicit source contracts, and the documented PowerShell 7 deployment/import procedures. This is a reusable procedure, not an outstanding task for the already accepted deployment.
