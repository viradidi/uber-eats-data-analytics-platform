# Final Project Handoff

## Project status

**Status: completed and historically executed end to end; cloud resources subsequently decommissioned to control costs.**

This repository is a reviewable, Azure-oriented data engineering portfolio project. Its committed artifacts cover a FastAPI mock source, sample data, API/CSV/SQL ingestion patterns, exported Azure Data Factory resources, Databricks Silver notebook exports, shared Spark utilities, data-quality checks, reconciliation utilities, operational SQL reports, CI validation, and recovery documentation.

The project owner confirms that the end-to-end implementation ran successfully in its original Azure environment. Those cloud resources were later intentionally decommissioned to avoid ongoing costs. The project is therefore complete, but it is not currently presented as a live hosted deployment.

This is owner-reported execution history, not independent third-party certification. Repository artifacts can be inspected directly; retained historical runtime evidence should be linked where available.

## What is delivered

- Multi-source ingestion design and exported ADF resource definitions.
- FastAPI mock source with API-key protection and source endpoints.
- Bounded SQL incremental extraction design, success-dependent checkpoint advancement, serialized pipeline runs, and a checkpoint compare-and-swap guard in the exported pipeline.
- Configurable Bronze roots and repository-owned shared Spark utilities for notebook portability.
- Data-quality checks, API contract tests, static ADF/recovery tests, repository artifact integrity tests, and GitHub Actions validation.
- Quarantine, quality metrics, reconciliation helpers, failure-event logging, operational SQL reports, and rerun/recovery guidance represented in the repository.
- Architecture, source contracts, concurrency contract, limitations, and portfolio evidence documentation.

## Documentation handoff

Start with the [README](../README.md), then use these documents for deeper review:

1. [Architecture overview](../architecture/architecture.md) and [architecture diagram](../architecture/uber-eats-end-to-end-architecture.svg)
2. [SQL incremental-load review](SQL_INCREMENTAL_LOAD_REVIEW.md)
3. [Incremental source contract](INCREMENTAL_SOURCE_CONTRACT.md)
4. [Checkpoint concurrency contract](CHECKPOINT_CONCURRENCY_CONTRACT.md)
5. [Operations and reliability](OPERATIONS_AND_RELIABILITY.md)
6. [Recovery and rerun runbook](RECOVERY_AND_RERUN_RUNBOOK.md)
7. [Portfolio evidence pack](PORTFOLIO_EVIDENCE.md)

## Validation and evidence boundaries

The project owner confirms successful end-to-end execution in the original environment. The repository also contains automated validation definitions and tests. These are different forms of evidence:

- **Historical cloud execution:** confirmed by the project owner; Azure resources are now decommissioned.
- **Repository validation:** inspect the workflow and tests, and check the GitHub Actions run history for a specific commit when a direct run record is needed.
- **Independent verification:** do not imply an external audit, current live deployment, production SLA, or certification unless separately supported by evidence.

Where retained, preserve real pipeline/job run records, reconciliation outputs, test results, and commit references. Do not invent run IDs, metrics, row counts, screenshots, or outcomes. If a particular artifact was not retained, say “not retained” rather than estimating.

## Engineering limitations to explain

The project is a strong implementation and design portfolio, but reviewers should be able to discuss its boundaries:

- A bounded upper watermark limits an extraction window; it does not by itself solve late visibility, equal or non-monotonic cursors, or deletes.
- Source correctness depends on an explicit cursor contract and suitable overlap, replay, CDC, or reconciliation strategy where required.
- Pipeline concurrency set to one serializes runs of that pipeline; it does not coordinate unrelated pipelines or external writers.
- Compare-and-swap protects checkpoint advancement against stale updates, but does not alone make landing and Silver writes idempotent.
- Dynamic SQL identifiers must be validated against an allowlist and protected control metadata.
- Repository tests do not substitute for a current live Azure integration run.

Use the linked source contracts and runbooks when discussing mitigations or future extensions. Do not describe proposed enhancements as already implemented.

## Safe portfolio description

> Built an Azure-oriented multi-source data engineering platform integrating API, CSV, and SQL sources through Azure Data Factory, ADLS Gen2, and Databricks/PySpark. Implemented metadata-driven ingestion, bounded incremental extraction, guarded checkpoint updates, transformation utilities, data-quality and reconciliation controls, operational reporting, and recovery documentation. The project ran successfully end to end in its original Azure environment; cloud resources were subsequently decommissioned to manage costs. Source code, configuration exports, tests, and engineering documentation remain available in GitHub.

Describe successful execution as project-owner-confirmed history. Reserve claims such as production-certified, independently audited, or guaranteed end-to-end idempotent for cases where specific acceptance criteria and supporting evidence establish them.

## No-cost posture and future operation

The source code, exported resource definitions, sample fixtures, tests, and documentation remain reviewable in GitHub. There is no requirement to recreate Azure resources just to keep the portfolio online or to produce screenshots.

If the project is redeployed in the future, use a budgeted environment, least-privilege identities, secret management, explicit source contracts, and the documented release gates. Re-run the relevant integration and recovery acceptance tests and retain the resulting evidence before making new runtime or performance claims.
