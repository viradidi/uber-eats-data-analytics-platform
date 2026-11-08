# Run Evidence and Retention

## Current validation status

The project owner confirms successful Azure deployment and end-to-end operation of the completed platform. The accepted scope includes API, CSV, and SQL ingestion; Bronze/Silver processing; repository-owned SQL Silver processing; incremental replay validation; Gold publication; and reconciliation. See the [Azure deployment and acceptance record](DEPLOYMENT_ACCEPTANCE_RECORD.md).

The latest verified repository validation is [GitHub Actions run #779](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/42021275338), with 135 tests passing on commit `034a19d5d62a94bb75e510f03e214f59590bf54a`. This is CI evidence for repository behavior and synthetic/local tests; it is not a substitute for Azure runtime evidence.

The precise Azure deployment date, ADF and Databricks run IDs, activity logs, runtime row counts, and reconciliation exports have not yet been attached to this repository. They are marked as unavailable in the acceptance record rather than inferred or invented. This is a documentation/evidence-retention gap, not an indication that deployment or end-to-end validation is pending.

## Evidence to retain from the accepted deployment

When exporting the actual records from Azure, preserve:

1. ADF master pipeline URL, pipeline name, run ID, start/end time, timezone, final status, and activity outcomes.
2. Child pipeline run IDs and activity outcomes for API, CSV, and SQL ingestion.
3. Databricks Silver and Gold notebook/job run URLs, run IDs, task output, and execution timestamps.
4. SQL source/control configuration snapshot with secrets and sensitive identifiers removed; include the approved source contract version and the tested watermark interval.
5. ADLS landing paths, schema snapshots, and actual source/landing/Silver counts where recorded.
6. The same-interval replay comparison, including the bounded interval, before/after target counts or checksums, and checkpoint state.
7. Silver quality/quarantine/reconciliation outputs and the relevant Gold `gold_run_audit` row.
8. Counts and reconciliation outputs for `gold_order_360`, `gold_daily_order_performance`, `gold_payment_method_performance`, `gold_restaurant_feedback`, and `gold_driver_performance`.
9. Any controlled failure/recovery evidence, but only for scenarios that were actually exercised.
10. The repository commit SHA and successful CI workflow URL corresponding to the deployed code.

## Evidence structure

Use `docs/run-evidence/runtime-evidence-template.md` for a redacted record. Store durable evidence in a controlled location with access appropriate to the data. Commit only safe summaries or screenshots that have been reviewed for secrets, personal information, customer data, and sensitive infrastructure details.

Do not put access tokens, credentials, connection strings, private endpoints, customer records, payment details, or unrestricted raw production-like exports in Git.

## What each evidence source proves

| Evidence source | Establishes | Does not establish by itself |
|---|---|---|
| Repository source, notebooks, SQL, and ADF exports | Implemented artifacts and design | Successful cloud execution |
| GitHub Actions | Repository test result for a specific commit | ADF/Databricks runtime behavior |
| Project-owner confirmation | Reported deployment and end-to-end acceptance | Independent third-party verification |
| ADF/Databricks run exports | Actual execution status and timestamps for the recorded run | Broad production certification or an SLA |
| Reconciliation outputs | The specific measured source/target checks performed | Correctness for untested data windows or future source changes |

## Known limitations

A successful run validates the configured and tested source path. It does not, by itself, prove universal late-arrival capture, hard-delete capture without CDC/tombstones/soft deletes, global exactly-once behavior, production certification, or continuous service availability. Keep the limitations in the SQL source contract and project handoff visible.

## CI artifact retention

The GitHub Actions workflow stores its test log as a workflow artifact with a 90-day retention period. Download and retain any CI artifact needed for a longer-lived review; do not rely on the default workflow artifact period as permanent archival storage.
