# Execution and validation notes

## End-to-end execution status

The project owner confirms that the restored Azure deployment of the latest enhancement set ran without runtime errors. The Gold pipeline completed, all five stable Gold views contain data, Gold quality metrics and publication history were persisted, the run audit was recorded, and the active-publication pointer advanced. The Gold retention notebook also completed a successful `dry_run=true` review without deleting data. See the [Azure deployment and acceptance record](DEPLOYMENT_ACCEPTANCE_RECORD.md).

The latest repository validation is [GitHub Actions run #830](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/45045827561), successful on `main` commit `231c949fae467eaec902b359be9d7baf1f693aac`. CI validates repository behavior and synthetic/local tests; it does not substitute for live Azure execution.

## What the repository preserves

- FastAPI source application and synthetic source data.
- Azure Data Factory pipeline, dataset, linked-service and factory exports.
- Databricks notebook exports and shared Spark transformation utilities.
- Gold analytics SQL and reconciliation logic.
- Automated tests and GitHub Actions validation.
- Architecture decisions, source contracts, operations and recovery documentation.

## Reading the validation layers

| Validation layer | Scope |
|---|---|
| Local API tests | API authentication, request/response contracts and source behavior |
| Local PySpark tests | Transformation logic against synthetic in-memory DataFrames |
| Static ADF tests | Exported JSON structure, resource references and pipeline dependency contracts |
| GitHub Actions | Repeatable repository validation for a specific commit |
| Completed Azure execution | Deployed API/CSV/SQL ingestion, Bronze/Silver processing, incremental replay, Gold publication and reconciliation |

## Operational boundaries

The earlier Azure resources were decommissioned to control cost and the environment was subsequently restored for the latest owner-confirmed acceptance. This is not a claim of continuous hosting, a production SLA, independent certification or universal exactly-once/CDC guarantees. Exact runtime run IDs, timestamps, row counts and exported evidence have not yet been committed; add sanitized artifacts when available.

Keep secrets, credentials, connection strings, personal data and sensitive infrastructure values out of source control.
