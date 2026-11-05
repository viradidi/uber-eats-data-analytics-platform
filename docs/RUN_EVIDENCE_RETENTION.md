# Run Evidence and Retention

## What the repository can prove

Keep repository-level evidence separate from cloud-runtime evidence.

- The GitHub Actions workflow validates committed JSON/notebook files, Python syntax, notebook code cells, Silver notebook portability, static ADF/Gold contracts, and the Python/PySpark unit tests.
- The workflow uploads its `test-results.txt` output as a GitHub Actions artifact with a 90-day retention period. The run is associated with its commit SHA and workflow URL. The latest verified full suite recorded 131 tests passing in [run 41983203349](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/41983203349), commit `c6e0c2cbe47dc3b16826653bf8d3e024963f7f99`.
- Original Azure run IDs, detailed run logs, and reconciliation exports were not retained after the original environment was decommissioned. The owner's confirmation of historical success is recorded separately and is not represented as independent run evidence.
- Existing Silver notebook logic appends quality, failure, quarantine, and reconciliation records to Delta audit tables where the corresponding workspace, catalog, schema, and permissions are available.
- The Gold notebook appends one row per execution to gold.gold_run_audit, including the parent ADF run ID, run status, output row counts, and UTC timestamps.

These mechanisms are implemented in the repository; the SQL Silver replacement, latest SQL hardening, Gold publication, and master orchestration still require a live environment rerun before current cloud runtime behavior can be claimed.

## Capture after the future Azure rerun

For every controlled end-to-end scenario, retain the actual evidence that exists:

1. ADF master pipeline URL, pipeline name, run ID, start/end time, and final status.
2. Child pipeline run IDs and activity outcomes for API, CSV, and SQL ingestion.
3. Databricks Gold notebook/job run URL and run ID.
4. The corresponding gold_run_audit row and output table counts.
5. Silver quality, failure, quarantine, and reconciliation rows relevant to the scenario.
6. Source-to-target reconciliation results and checkpoint values before/after incremental runs.
7. Failure/retry/rerun evidence when those scenarios are deliberately tested.

Use docs/run-evidence/runtime-evidence-template.md to record each scenario. Add sanitized screenshots, exported JSON/CSV summaries, or direct run URLs under docs/run-evidence/ only when they contain real observations. Never add API keys, connection strings, account identifiers, real customer records, or unredacted infrastructure secrets.

## What not to claim

- A successful local unit-test run is not a successful ADF or Databricks runtime run.
- A valid ADF export is not proof the pipeline has been published or executed.
- A run-audit table with no row does not imply a successful run.
- Do not invent run IDs, timestamps, counts, duration, throughput, failure scenarios, or output screenshots.
- If the original run evidence was not retained, state that it is not retained. The owner's report of historical end-to-end execution is separate from newly generated evidence.

## Retention policy

GitHub Actions test artifacts are configured for 90 days. Preserve important future Azure run evidence outside the ephemeral compute session in a sanitized, access-controlled location; commit only a redacted summary or safe artifact that is useful for portfolio review. Do not commit raw production-like data.
