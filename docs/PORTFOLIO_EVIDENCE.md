# Project review guide

## Project status

The Uber Eats Data Engineering Platform was successfully deployed and executed end to end in Azure. For the restored latest enhancement deployment, the project operator confirmed error-free component execution, populated five stable Gold views, persisted quality metrics/publication history/run audit, an updated active-publication pointer, and a successful non-destructive retention dry run. See the [Azure deployment and acceptance record](DEPLOYMENT_ACCEPTANCE_RECORD.md).

The latest repository CI is [GitHub Actions run #830](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/45045827561), with all workflow steps passing on `main` commit `231c949fae467eaec902b359be9d7baf1f693aac`. CI validates repository code and local/synthetic tests; the Azure deployment and end-to-end execution were completed separately. Historical baseline: [run #779](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/42021275338) passed 135 tests on 5 November 2026.

## What a reviewer can inspect

| Area | Repository entry point | What it describes |
|---|---|---|
| Architecture | `architecture/` | Source systems, ADF orchestration, ADLS landing, Databricks transformation and Gold extension |
| Ingestion | `pipeline/`, `dataset/`, `linkedService/` | API, CSV and SQL ingestion definitions and their dependencies |
| API source | `main.py`, `data/` | Authenticated mock API and synthetic restaurant-delivery source records |
| Transformations | `notebooks/`, `src/` | Silver and Gold PySpark transformation logic |
| Analytics | `reports/`, `docs/GOLD_LAYER_AND_REPORTING.md` | Gold data products, metric grain and SQL reporting |
| Incremental processing | `docs/SQL_INCREMENTAL_LOAD_REVIEW.md`, `docs/INCREMENTAL_SOURCE_CONTRACT.md` | Watermarks, bounded extraction, checkpoint advancement and replay assumptions |
| Reliability | `docs/OPERATIONS_AND_RELIABILITY.md`, `docs/RECOVERY_AND_RERUN_RUNBOOK.md` | Data quality, reconciliation, recovery decisions and failure boundaries |
| Automated validation | `.github/workflows/`, `tests/` | Repository checks, API contracts and local Spark transformation tests |

## Engineering capabilities demonstrated

1. Multi-source ingestion across REST API, CSV and relational SQL.
2. Metadata-configured orchestration and repository-owned Databricks notebook dispatch.
3. Bounded incremental extraction with checkpoint ordering and compare-and-swap protection.
4. Spark transformations with schema, key, domain and data-quality checks.
5. Gold analytical modelling with documented grains and reconciliation rules.
6. Operational failure handling, audit records and recovery procedures.

## Validation interpretation

The repository's automated tests validate code, configuration contracts and local/synthetic behavior. They do not provision or execute Azure Data Factory and Databricks services. The restored Azure workflow was separately deployed and executed end to end; the detailed runtime run exports have not yet been attached.

Azure resources are not continuously hosted. Describe this as a completed implementation and owner-confirmed deployment, not a continuously hosted service or a production SLA.

## Senior-review discussion

The strongest technical discussion topics are:

- How the API, CSV and SQL source contracts differ.
- Why SQL extraction uses a bounded watermark and how checkpoint advancement is ordered after downstream success.
- Where watermark processing stops being sufficient and CDC or reconciliation becomes necessary.
- How the Silver merge logic handles duplicate keys, source versions and replay.
- Why Gold outputs use their documented grains and how payment metrics are reconciled.
- Which reliability guarantees are implemented and which are bounded by source or environment assumptions.
