# Project Release Gate

## Release decision

**Portfolio status: COMPLETED.** The project owner confirms that the end-to-end implementation ran successfully in its original Azure environment. The Azure resources were subsequently decommissioned to control costs; no live deployment is currently available.

This is owner-reported execution history. It is not an independent audit, production certification, or proof that the environment is still running. Repository tests validate committed behaviors and static contracts; they do not execute ADF or Databricks in Azure.

## Delivered scope

- FastAPI mock source API and synthetic domain data.
- Exported Azure Data Factory definitions for API, CSV, and SQL ingestion patterns.
- ADLS Gen2 landing integration and Databricks Silver notebook exports.
- SQL bounded-watermark extraction, serialized pipeline configuration, and compare-and-swap-style checkpoint update.
- Shared PySpark utilities, data-quality and reconciliation helpers, operational SQL reports, CI checks, and recovery documentation.

## Evidence and validation boundaries

| Area | What is present | Evidence boundary |
|---|---|---|
| Repository integrity | JSON/notebook exports, Python source, automated tests, GitHub Actions workflow | A specific CI run must be checked for its commit and conclusion |
| API behavior | FastAPI implementation and endpoint/security tests | Local tests do not prove an Azure-hosted API deployment |
| ADF orchestration | Exported pipeline, dataset, and linked-service definitions | Export inspection is not a current ADF runtime execution |
| Incremental SQL | Bounded extraction query, watermark control, preflight and source contracts | Correctness depends on source-specific cursor semantics and deployment safeguards |
| Databricks/Silver | Notebook exports, shared utilities, validation and reconciliation code | Notebook source and syntax checks do not prove current Spark runtime execution |
| Historical end-to-end run | Project owner confirms successful end-to-end execution before resource teardown | Detailed run IDs, row counts, and scenario-specific logs should only be claimed where retained |
| Replay and recovery | Runbooks and static dependency tests | Do not claim universal exactly-once or replay safety without scenario-specific evidence |

## Deployment gates before reuse

Before redeploying or enabling a source in a new environment:

1. Validate the source table, business key, cursor type/precision, timezone, initial-load behavior, late visibility, equal cursor values, and delete semantics.
2. Enforce an approved allowlist for dynamic SQL identifiers and restrict control-table write access.
3. Confirm typed watermark serialization and null-checkpoint handling.
4. Verify landing write behavior and replay-safe Silver merge/deduplication semantics.
5. Test copy failure, Silver failure, checkpoint conflict, reconciliation mismatch, and safe rerun in the target environment.
6. Configure identities, least-privilege permissions, and secret-backed connection settings.
7. Retain actual run IDs, commit SHAs, counts, reconciliation results, and test outputs for any claims made.

These are redeployment acceptance gates, not a request to recreate cloud resources solely for portfolio presentation.

## Portfolio claims

Safe description: **an Azure-oriented end-to-end data engineering platform integrating API, CSV, and SQL sources through ADF, ADLS Gen2, and Databricks/PySpark, with incremental-load controls, quality/reconciliation utilities, and operational recovery documentation. The project ran successfully in its original environment; Azure resources were later decommissioned to manage costs.**

Do not claim a currently deployed service, independent production certification, guaranteed exactly-once processing, or unmeasured performance outcomes.
