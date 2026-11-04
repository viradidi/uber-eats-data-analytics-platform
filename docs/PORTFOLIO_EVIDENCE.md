# Portfolio Evidence Pack

## Purpose

This guide helps a hiring manager or technical reviewer inspect the engineering work in this repository and understand what was validated.

The project owner confirms that the original API/CSV/SQL ingestion and Silver platform ran successfully end to end in its original Azure environment. Azure resources were later decommissioned to control costs. The Gold analytics layer, business-reporting SQL, master ADF pipeline, and local Spark tests are newer repository additions and await a fresh Azure rerun; do not imply that this newer scope has already executed in the cloud.

Keep three kinds of evidence distinct:

- **Repository evidence** — source code, exported pipeline/configuration files, notebooks, SQL, tests, and documentation committed here.
- **Historical runtime evidence** — retained run records, screenshots, logs, or outputs from the original Azure/Databricks environment, if available.
- **Owner-reported execution history** — the project owner's confirmation that the end-to-end implementation ran successfully. This is useful context, but it is not a substitute for a retained run record that an external reviewer can independently inspect.

Do not invent run IDs, metrics, screenshots, dashboard states, or test outcomes. Do not redeploy Azure resources solely to recreate portfolio evidence.

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
6. Follow the SQL incremental-load and checkpoint contracts:
   - [SQL incremental-load review](SQL_INCREMENTAL_LOAD_REVIEW.md)
   - [Incremental source contract](INCREMENTAL_SOURCE_CONTRACT.md)
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

## Historical and future runtime evidence

If original artifacts were retained, these are the most useful items to include. Add only real, sanitized records.

| Priority | Artifact | Useful details |
|---|---|---|
| High | ADF successful pipeline run | Pipeline name, run ID, timestamp/timezone, final status, and completed activities |
| High | Databricks successful job/notebook run | Job or notebook name, run ID, timestamp, final status, target table/layer, and row counts if recorded |
| High | Reconciliation output | Actual source/target counts or checks, expected result, observed result, and query/version |
| High | GitHub Actions run | Direct URL to the successful run, commit SHA, workflow name, and conclusion |
| Medium | Incremental-load scenario | Initial and subsequent run details, source changes, expected/observed counts, and checkpoint before/after |
| Medium | Recovery/rerun scenario | Failure condition, recovery action, rerun result, and duplicate/checkpoint observations |
| Medium | Data-quality or monitoring output | Actual metrics, rejected/quarantined rows, freshness, time window, and outcome |
| Optional | Sanitized output sample | Representative synthetic or approved rows and schema, with sensitive fields removed |

A generic [GitHub Actions page](https://github.com/viradidi/uber-eats-data-analytics-platform/actions) is a starting point for locating workflow runs; link to a specific successful run only after verifying that run and its commit.

If no historical runtime artifact was retained, say so plainly. The project owner's confirmation of successful execution can be stated as project history, but it must not be presented as independently verified evidence.

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

Suggested portfolio wording:

> Built and validated an end-to-end Azure-oriented data engineering platform integrating API, CSV, and SQL sources through Azure Data Factory, ADLS Gen2, and Databricks/PySpark. Implemented metadata-driven ingestion, bounded incremental extraction, guarded checkpoint updates, transformation utilities, data-quality and reconciliation controls, and operational recovery documentation. The project ran successfully in its original environment; Azure resources were subsequently decommissioned to manage costs. Source code, configuration exports, tests, and engineering documentation remain available in the repository.

Use wording that distinguishes the previously executed ingestion/Silver platform from the newer Gold extension awaiting live validation. The repository has reporting SQL but no deployed Power BI dashboard. Tailor any quantified impact or performance claims only when measured results are available.


## Gold extension evidence boundary

The Gold transformation module is tested locally with PySpark in CI, while the master ADF pipeline is checked structurally for its child-pipeline success dependencies and Gold notebook path. Those are meaningful code-level checks but are not an Azure integration test. See [run evidence and retention](RUN_EVIDENCE_RETENTION.md) for the evidence to capture during the future environment rerun.
