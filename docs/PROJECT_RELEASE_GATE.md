# Project Release Gate

## Release decision

**Status: COMPLETED — end-to-end implementation and validation confirmed by the project owner. Portfolio release is complete.** The owner confirms the project workflows and checks are working. This records project validation, not independent audit or production certification.

The project owner confirms the workflows and checks are working. Implementation and validation are complete. Existing test output, pipeline run IDs, and reconciliation results may be retained as supporting reviewer evidence; evidence collection is not an outstanding release gate.

## What is in scope and represented in the repository

- FastAPI mock source API and sample domain data.
- Exported Azure Data Factory definitions for API, CSV, and SQL ingestion patterns.
- SQL incremental extraction configuration with bounded upper watermark, deterministic landing-batch naming, serialized pipeline runs, and a prior-checkpoint comparison before checkpoint advancement.
- Databricks Silver notebook exports and shared Spark utilities.
- Sample-data quality checks, static ADF/recovery contract checks, reconciliation utilities, operational SQL reports, and recovery documentation.

## Completed acceptance gates

| Gate | Evidence required | Current disposition |
|---|---|---|
| Repository integrity | Tests run locally/CI; JSON and notebook exports parse; links resolve | Passing repository test suite; project owner reports checks are passing |
| API and fixtures | Endpoint tests, malformed-source behavior, fixture quality checks | Covered by committed tests; project owner reports validation passed |
| ADF export | Dependency graph, parameter references, linked services, datasets, expressions, and SQL metadata reviewed | Static review artifacts exist; deployment validation completed by project owner |
| SQL incremental correctness | Per-source key/watermark contract, initial-load strategy, late/equal timestamp behavior, delete semantics | Reviewed and validated by project owner for enabled sources |
| Checkpoint safety | Only advance after successful Silver processing; stale checkpoint conflict fails visibly | Represented in the export and validated by project owner in the target environment |
| Replay safety | Re-run the same batch; prove landing behavior and Silver upsert/deduplication produce no duplicate business keys | Deterministic filename is implemented; replay behavior validated by project owner for the tested workflows |
| Silver transformations | Execute notebooks on representative fixtures; verify schemas, key constraints, quarantine, reconciliation, and target writes | Notebook exports and static tests exist; Spark runtime execution validated by project owner |
| Recovery | Inject copy failure and Silver failure; verify checkpoint does not advance prematurely; rerun successfully | Runbook exists; failure/recovery scenarios validated by project owner |
| Security | Scan for secrets; validate least privilege, allowlisted SQL metadata, and runtime identity | Documentation and environment-specific security review completed by project owner |
| Operations | Capture run IDs, counts, quality outcomes, reconciliation, and failure evidence | Operational SQL/report artifacts exist; operational behavior validated by project owner |

## Optional evidence preservation

The release is complete. The following are optional evidence-preservation steps and do not block completion; they can be done without recreating Azure resources:

1. From the repository root, run the project's documented test command and save the terminal output.
2. Parse all committed JSON exports and check for unresolved merge markers, accidental credentials, and broken relative README links.
3. Review the API route tests, fixture-quality tests, and `tests/test_recovery_contract.py` as separate layers; report the actual pass/fail counts.
4. Inspect the ADF export against `docs/SQL_INCREMENTAL_CONTROL_PREFLIGHT.sql` and the per-source contract. Treat source table/column allowlisting and SQL value quoting as deployment gates, not documentation-only fixes.
5. Review each Silver notebook's input/output contract and ensure the runbook clearly distinguishes what is statically tested from what needs Spark execution.
6. Keep the README aligned with the validated status and distinguish functional validation from any acceptance criteria not exercised.

## Validation evidence to retain (optional)

The project owner confirms that the workflows and checks have been validated. Retain available evidence for first load, no-change and changed-row runs, watermark boundaries, copy/Silver failures, same-range retries, duplicate keys, checkpoint conflicts, reconciliation mismatches, and successful recovery where those scenarios were part of the validation.

## Claims guidance

Safe description: **an Azure-oriented end-to-end data-platform portfolio project with exported orchestration, API and file ingestion, SQL incremental-load design, Silver notebook implementations, quality/reconciliation utilities, and operational runbooks.**

Do not claim production certification or universal end-to-end idempotency beyond the scope of the scenarios actually validated.