# Portfolio Evidence Pack

## Purpose

This guide helps a hiring manager or technical reviewer inspect the engineering work and understand what was validated.

**Current runtime status:** the project owner confirms that the completed platform has been successfully deployed to Azure and verified end to end. The confirmed flow includes API, CSV, and SQL ingestion; Bronze/Silver processing; the repository-owned SQL Silver path; incremental replay validation; Gold publication; and reconciliation. The five documented Gold outputs are listed in the [Azure deployment and acceptance record](DEPLOYMENT_ACCEPTANCE_RECORD.md).

The latest repository CI evidence is [GitHub Actions run 41984330196](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/41984330196), with 132 tests passing on commit `9ec8d65863f3a110119cb47e9789f39ec545c3d8`. CI and live runtime acceptance are different evidence types: CI validates repository code/tests, while the Azure deployment and runtime outcome are project-owner-confirmed.

Exact ADF/Databricks run IDs, timestamps, runtime row counts, and exported logs have not yet been attached to the repository. The [acceptance record](DEPLOYMENT_ACCEPTANCE_RECORD.md) marks them as not supplied rather than inventing values. This is an evidence-retention gap, not a pending deployment gate.

Keep three kinds of evidence distinct:

- **Repository evidence** — source code, exported pipeline/configuration files, notebooks, SQL, tests, and documentation committed here.
- **Runtime acceptance** — the project owner's confirmation that the deployed API/CSV/SQL-to-Gold flow executed and was reconciled end to end.
- **Retained runtime artifacts** — original ADF/Databricks run records, screenshots, logs, and reconciliation outputs that can be inspected independently when exported and sanitized.

Do not invent run IDs, metrics, screenshots, dashboard states, or test outcomes. Do not imply independent certification or continuous production availability.

## What the project demonstrates

The strongest interview discussion areas are the engineering decisions behind the implementation:

1. **Ingestion design:** coordinating API, CSV, and relational SQL inputs through a common orchestration and landing approach.
2. **Metadata-driven processing:** separating source configuration and control state from pipeline activity definitions.
3. **Incremental extraction:** using a captured upper watermark to bound an extraction window, then advancing state only after the downstream processing dependency succeeds.
4. **Concurrency control:** combining pipeline-level serialization with a compare-and-swap checkpoint update, while recognizing the limits of each control.
5. **Data processing:** applying PySpark transformations and shared standardization utilities in the lake processing layer.
6. **Analytics engineering:** building a Gold order-level model and daily, payment-method, driver, and restaurant-feedback aggregates, with documented metric grains and SQL reporting queries.
7. **Operational readiness:** providing quality, reconciliation, failure logging, run-level audit records, evidence retention, recovery, and rerun guidance rather than treating a successful ingestion as the whole solution.

Be prepared to explain the trade-offs and limitations as well as the implementation. In particular, discuss late-arriving rows, equal or non-monotonic cursors, deletes, replay safety, dynamic SQL identifier validation, and the difference between repository tests and cloud-runtime validation.

## Evidence map

| Engineering claim | Evidence a reviewer can inspect | What it establishes | What it does not establish by itself |
|---|---|---|---|
| Multi-source ingestion is implemented | FastAPI source API, ADF pipeline and dataset definitions, SQL/CSV/API ingestion assets | The ingestion design and implementation artifacts are present in the repository | That a live Azure pipeline is currently deployed |
| Data is landed in the lake | ADLS-related linked services, datasets, pipeline activities, and landing conventions | The configured landing path and orchestration intent | Current storage contents or live resource availability |
| Silver and Gold Spark transformations are implemented | Databricks/PySpark notebooks, src/gold/transformations.py, and local-mode PySpark tests | Transformation logic and tested behavior against synthetic in-memory fixtures | A current Databricks job run or successful Gold Delta publication |
| Incremental extraction is controlled | SQL extraction query, control/checkpoint logic, and [SQL incremental-load review](SQL_INCREMENTAL_LOAD_REVIEW.md) | Bounded watermark extraction and the documented checkpoint design | Correct handling of every source's late-arriving updates or deletes without the source contract being met |
| Concurrent checkpoint writes are guarded | ADF concurrency setting, compare-and-swap update logic, and [checkpoint concurrency contract](CHECKPOINT_CONCURRENCY_CONTRACT.md) | The implemented serialization and optimistic checkpoint guard | Protection from unrelated pipelines or external writers that bypass the same control |
| Data quality and operational handling are considered | Quality/reconciliation utilities, operational SQL, failure logging, and runbooks | The available controls and documented response procedures | A specific quality threshold or recovery scenario passing unless its result is retained |
| Gold orchestration and business reporting are defined | pipeline/uber_eats_master.json, Gold notebook exports, and reports/GOLD_ANALYTICS_REPORTING.sql | The static ADF dependency contract and SQL reporting definitions | A published ADF pipeline, live notebook execution, or deployed Power BI dashboard |
| Repository changes are validated automatically | The validation workflow definition and test suite | The automated validation workflow and test definitions | A successful run for a particular commit unless the corresponding Actions run is inspected |

## Recommended reviewer path

A reviewer can follow this sequence without access to the retired cloud environment:

1. Start with the [README](../README.md) for scope, architecture, setup, and project status.
2. Inspect the [architecture diagram](../architecture/uber-eats-end-to-end-architecture.svg) and its [technical explanation](../architecture/architecture.md).
3. Trace an ingestion path through the ADF pipeline, datasets, linked services, and API implementation.
4. Review the Silver Databricks notebooks, src/gold/transformations.py, and Gold_Analytics notebook export.
5. Review [Gold table grains and metric definitions](GOLD_LAYER_AND_REPORTING.md) and the [business reporting SQL pack](../reports/GOLD_ANALYTICS_REPORTING.sql).
6. Follow the SQL incremental-load, source inventory, and checkpoint contracts:
   - [SQL incremental-load review](SQL_INCREMENTAL_LOAD_REVIEW.md)
   - [Incremental source contract](INCREMENTAL_SOURCE_CONTRACT.md)
   - [Read-only source catalog and key inventory](../sql/diagnostics/inspect_source_contract_candidates.sql)
   - [Notebook export consistency](NOTEBOOK_EXPORT_CONSISTENCY.md)
   - [Checkpoint concurrency contract](CHECKPOINT_CONCURRENCY_CONTRACT.md)
7. Review [operations and reliability](OPERATIONS_AND_RELIABILITY.md), the [recovery and rerun runbook](RECOVERY_AND_RERUN_RUNBOOK.md), and [run evidence retention](RUN_EVIDENCE_RETENTION.md).
8. Inspect the validation workflow and tests, then distinguish local Spark/static contract tests from historical cloud execution.

## Interview discussion prompts

Use these prompts to test whether the implementation can be explained clearly in a technical interview:

- Why use a captured upper watermark instead of extracting only with a lower-bound predicate?
- What happens if a source row becomes visible late with a cursor value at or below the committed checkpoint?
- Why is checkpoint advancement dependent on successful downstream processing?
- What does pipeline concurrency set to one protect, and what does it not protect?
- How does compare-and-swap detect a stale checkpoint update?
- Why does a deterministic landing filename not, by itself, guarantee idempotent processing?
- How would you extend the design to support deletes, CDC, schema evolution, and replay-safe merges?
- Which checks run in CI, and which require a live Azure integration environment?

Answers should refer to the actual repository implementation and its documented boundaries, not imply that every proposed extension is already implemented.

## Accepted deployment evidence and remaining evidence capture

The end-to-end deployment and operation are owner-confirmed as successful. The project evidence pack still benefits from attaching the original sanitized runtime records:

| Priority | Artifact | Useful details | Current state |
|---|---|---|---|
| High | ADF master pipeline run | Pipeline name, run ID, timestamp/timezone, final status, completed activities | Successful run owner-confirmed; exact record not attached |
| High | ADF child pipeline runs | API, CSV, and SQL child run IDs and activity outcomes | Successful flow owner-confirmed; exact IDs not attached |
| High | Databricks Silver/Gold runs | Notebook/job names, run IDs, timestamps, final status, target outputs | Runtime flow owner-confirmed; exact IDs not attached |
| High | Reconciliation output | Actual source/target checks, expected/observed result, query/version | Reconciliation owner-confirmed; raw output not attached |
| Medium | Incremental replay scenario | Tested lower/upper interval, source changes, before/after target comparison, checkpoint state | Replay validation owner-confirmed; comparison export not attached |
| Medium | Gold outputs and audit | Five Gold table counts and the corresponding `gold_run_audit` record | Gold publication owner-confirmed; exact counts/row not attached |
| Medium | Recovery/rerun scenario | Failure condition, recovery action, rerun result, duplicate/checkpoint observations | Record only scenarios actually exercised |
| High | GitHub Actions run | Direct URL, commit SHA, workflow name, conclusion | [132 tests passed in run 41984330196](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/41984330196) |

The absence of raw Azure exports does not reverse the successful deployment status. It limits the independent inspectability and quantitative detail of the portfolio evidence.

## Evidence record template

Copy this template for each scenario for which actual evidence exists:

- **Scenario:**
- **Environment:**
- **Date/time and timezone:**
- **Commit or version:**
- **Pipeline, job, notebook, or test command:**
- **Run ID or direct workflow URL:**
- **Expected outcome:**
- **Observed outcome:**
- **Rows read/written/updated/rejected (if recorded):**
- **Reconciliation result:**
- **Checkpoint before/after (if applicable):**
- **Evidence file or link:**
- **Limitations or notes:**

Do not fill unknown values with estimates. Use “not retained” or “not recorded” where appropriate.

## Capture and redaction rules

- Preserve enough context to identify the environment, run, status, and timestamp.
- Prefer direct links to authoritative run records; use screenshots as supporting material.
- Redact API keys, tokens, passwords, connection strings, private tenant/subscription identifiers, and personal or customer data.
- Use synthetic or explicitly approved sample data. Never publish real customer or payment records.
- Do not treat a green unit-test run as proof of live ADF or Databricks execution.
- Do not treat exported JSON, notebook source, or a diagram as proof that a resource is currently deployed.
- Do not claim a recovery test, reconciliation, or performance target passed unless the actual result was recorded.
- Preserve historical evidence if available, but do not recreate cloud infrastructure solely for a screenshot.

## How to describe the project accurately

> Deployed and verified an Azure-based multi-source data engineering platform integrating API, CSV, and SQL ingestion through Azure Data Factory, ADLS Gen2, and Databricks/PySpark. Implemented Bronze, Silver, and Gold processing, incremental SQL ingestion with checkpoint controls and replay validation, data-quality and reconciliation checks, operational audit records, and master-pipeline orchestration. The project owner confirmed successful end-to-end operation after deployment; the repository validation suite also passed 132 tests.

Only add measured row counts, runtime durations, throughput, cost savings, or performance improvements after attaching the actual supporting outputs. Do not claim independent audit, production certification, or an SLA unless separately established.

## Gold extension evidence boundary

The Gold extension is no longer awaiting deployment according to the project owner's confirmation. The deployed end-to-end flow includes the five documented Gold outputs and reconciliation. The repository contains the reusable Gold transformations, notebook export, ADF master-pipeline definition, audit contract, and business reporting SQL. Exact Gold row counts, Databricks run IDs, and the corresponding `gold_run_audit` export remain to be attached when available.

The repository contains reporting SQL, not a deployed Power BI dashboard. The logical architecture SVG remains an ingestion/Silver view; the Gold extension is documented in [Gold layer and reporting](GOLD_LAYER_AND_REPORTING.md) and [architecture](../architecture/architecture.md).


The Gold transformation module is tested locally with PySpark in CI, while the master ADF pipeline is checked structurally for its child-pipeline success dependencies and Gold notebook path. Those are meaningful code-level checks but are not an Azure integration test. See [run evidence and retention](RUN_EVIDENCE_RETENTION.md) for the evidence to capture during the future environment rerun.
