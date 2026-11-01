# Portfolio Evidence Pack

## Purpose

This guide helps a hiring manager or technical reviewer inspect the engineering work in this repository and understand what was validated.

The project owner confirms that the platform was completed end to end and ran successfully in its original Azure environment. Azure resources were later decommissioned to control costs. The repository therefore represents a completed project with a historical execution record, not a currently running hosted service.

Keep three kinds of evidence distinct:

- **Repository evidence** — source code, exported pipeline/configuration files, notebooks, SQL, tests, and documentation committed here.
- **Historical runtime evidence** — retained run records, screenshots, logs, or outputs from the original Azure/Databricks environment, if available.
- **Owner-reported execution history** — the project owner's confirmation that the end-to-end implementation ran successfully. This is useful context, but it is not a substitute for a retained run record that an external reviewer can independently inspect.

Do not invent run IDs, metrics, screenshots, dashboard states, or test outcomes. Do not redeploy Azure resources solely to recreate portfolio evidence.

## Evidence map

| Engineering claim | Evidence a reviewer can inspect | What it establishes | What it does not establish by itself |
|---|---|---|---|
| Multi-source ingestion is implemented | FastAPI source API, ADF pipeline and dataset definitions, SQL/CSV/API ingestion assets | The ingestion design and implementation artifacts are present in the repository | That a live Azure pipeline is currently deployed |
| Data is landed in the lake | ADLS-related linked services, datasets, pipeline activities, and landing conventions | The configured landing path and orchestration intent | Current storage contents or live resource availability |
| Spark transformations are implemented | Databricks/PySpark notebooks and shared utilities | Transformation logic, standardization, and processing structure | A current Databricks job run |
| Incremental extraction is controlled | SQL extraction query, control/checkpoint logic, and [SQL incremental-load review](SQL_INCREMENTAL_LOAD_REVIEW.md) | Bounded watermark extraction and the documented checkpoint design | Correct handling of every source's late-arriving updates or deletes without the source contract being met |
| Concurrent checkpoint writes are guarded | ADF concurrency setting, compare-and-swap update logic, and [checkpoint concurrency contract](CHECKPOINT_CONCURRENCY_CONTRACT.md) | The implemented serialization and optimistic checkpoint guard | Protection from unrelated pipelines or external writers that bypass the same control |
| Data quality and operational handling are considered | Quality/reconciliation utilities, operational SQL, failure logging, and runbooks | The available controls and documented response procedures | A specific quality threshold or recovery scenario passing unless its result is retained |
| Repository changes are validated automatically | The validation workflow definition and test suite | The automated validation workflow and test definitions | A successful run for a particular commit unless the corresponding Actions run is inspected |

## Recommended reviewer path

A reviewer can follow this sequence without access to the retired cloud environment:

1. Start with the [README](../README.md) for scope, architecture, setup, and project status.
2. Inspect the [architecture diagram](../architecture/uber-eats-end-to-end-architecture.svg) and its [technical explanation](../architecture/architecture.md).
3. Trace an ingestion path through the ADF pipeline, datasets, linked services, and API implementation.
4. Review the Databricks notebooks and shared PySpark utilities.
5. Follow the SQL incremental-load and checkpoint contracts:
   - [SQL incremental-load review](SQL_INCREMENTAL_LOAD_REVIEW.md)
   - [Incremental source contract](INCREMENTAL_SOURCE_CONTRACT.md)
   - [Checkpoint concurrency contract](CHECKPOINT_CONCURRENCY_CONTRACT.md)
6. Review [operations and reliability](OPERATIONS_AND_RELIABILITY.md) and the [recovery and rerun runbook](RECOVERY_AND_RERUN_RUNBOOK.md).
7. Inspect the validation workflow and tests, then distinguish automated repository checks from historical cloud execution.

## Historical runtime evidence

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

Use this wording as a concise project summary, not as a claim that the cloud deployment is currently live. Tailor any quantified impact or performance claims only when measured results are available.
