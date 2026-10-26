# Project Release Gate

## Release decision

**Status: repository implementation artifacts are ready for a final review; live-cloud release is not certified.** This is the honest completion boundary for this portfolio project while its Azure resources are not running.

The project should be treated as portfolio-complete when the checked-in artifacts, tests, architecture, operating guidance, and known limitations are presented coherently. Do not label the Azure pipeline runtime-validated until it has actually been deployed and exercised.

## What is in scope and represented in the repository

- FastAPI mock source API and sample domain data.
- Exported Azure Data Factory definitions for API, CSV, and SQL ingestion patterns.
- SQL incremental extraction configuration with bounded upper watermark, deterministic landing-batch naming, serialized pipeline runs, and a prior-checkpoint comparison before checkpoint advancement.
- Databricks Silver notebook exports and shared Spark utilities.
- Sample-data quality checks, static ADF/recovery contract checks, reconciliation utilities, operational SQL reports, and recovery documentation.

## Final acceptance gates

| Gate | Evidence required | Current disposition |
|---|---|---|
| Repository integrity | Tests run locally/CI; JSON and notebook exports parse; links resolve | Run the repository test suite and record its actual result before publishing a pass claim |
| API and fixtures | Endpoint tests, malformed-source behavior, fixture quality checks | Covered by committed tests; do not infer pass status without a recorded run |
| ADF export | Dependency graph, parameter references, linked services, datasets, expressions, and SQL metadata reviewed | Static review artifacts exist; deployment validation remains pending |
| SQL incremental correctness | Per-source key/watermark contract, initial-load strategy, late/equal timestamp behavior, delete semantics | Must be decided per source before enabling that source |
| Checkpoint safety | Only advance after successful Silver processing; stale checkpoint conflict fails visibly | Represented in the export; runtime semantics and value quoting/type behavior still require validation |
| Replay safety | Re-run the same batch; prove landing behavior and Silver upsert/deduplication produce no duplicate business keys | Deterministic filename is implemented; end-to-end replay safety is not yet proven |
| Silver transformations | Execute notebooks on representative fixtures; verify schemas, key constraints, quarantine, reconciliation, and target writes | Notebook exports and static tests exist; Spark runtime execution remains pending |
| Recovery | Inject copy failure and Silver failure; verify checkpoint does not advance prematurely; rerun successfully | Runbook exists; live failure/recovery scenarios remain pending |
| Security | Scan for secrets; validate least privilege, allowlisted SQL metadata, and runtime identity | Documentation exists; environment-specific security review remains pending |
| Operations | Capture run IDs, counts, quality outcomes, reconciliation, and failure evidence | Operational SQL/report artifacts exist; actual run evidence remains pending |

## No-cost finalization workflow

These steps can be completed without recreating Azure resources:

1. From the repository root, run the project's documented test command and save the terminal output.
2. Parse all committed JSON exports and check for unresolved merge markers, accidental credentials, and broken relative README links.
3. Review the API route tests, fixture-quality tests, and `tests/test_recovery_contract.py` as separate layers; report the actual pass/fail counts.
4. Inspect the ADF export against `docs/SQL_INCREMENTAL_CONTROL_PREFLIGHT.sql` and the per-source contract. Treat source table/column allowlisting and SQL value quoting as deployment gates, not documentation-only fixes.
5. Review each Silver notebook's input/output contract and ensure the runbook clearly distinguishes what is statically tested from what needs Spark execution.
6. Update the README with the actual test command, actual result, and a short list of unexecuted cloud acceptance gates.

## Cloud acceptance when an environment is available

Use a non-production source and a small controlled batch. Capture evidence for: first load; no-change run; changed rows; equal/late watermark values; copy failure; Silver failure; retry of the same range; duplicate business keys; checkpoint conflict; reconciliation mismatch; and successful recovery. Confirm the checkpoint advances only after the target write and reconciliation gates succeed.

## Claims guidance

Safe description: **an Azure-oriented end-to-end data-platform portfolio project with exported orchestration, API and file ingestion, SQL incremental-load design, Silver notebook implementations, quality/reconciliation utilities, and operational runbooks.**

Do not claim a currently deployed, production-certified, end-to-end idempotent, or live-tested Azure platform until the corresponding acceptance evidence exists.