# Azure deployment and end-to-end execution record

## Completion status

**Completed — successfully deployed and executed end to end in Azure; latest enhancement deployment operator-confirmed on 22 November 2026.**

The completed Uber Eats platform covers API, CSV and SQL ingestion; Bronze and Silver processing; the repository-owned SQL Silver path; incremental replay validation; Gold publication; the master ADF orchestration; and reconciliation.

The five documented Gold outputs are:

- `gold_order_360`
- `gold_daily_order_performance`
- `gold_payment_method_performance`
- `gold_restaurant_feedback`
- `gold_driver_performance`

The resources from the earlier Azure deployment were decommissioned to control ongoing costs. The Azure environment was subsequently restored for the latest enhancement deployment and acceptance on 22 November 2026. The owner confirms the restored deployment ran without runtime errors. This record does not imply continuous hosting or guaranteed ongoing availability. The repository preserves the source code, ADF definitions, Databricks notebooks, SQL, tests, architecture and operational documentation needed to understand and redeploy the platform.

## Completed execution scope

| Scope | Completed behavior |
|---|---|
| Azure deployment | Platform deployed in the intended Azure environment |
| API, CSV and SQL ingestion | All configured ingestion paths executed |
| Bronze-to-Silver processing | Source records processed through the configured Silver paths |
| Repository-owned SQL Silver | Incremental SQL notebook included in the deployed workflow |
| Incremental replay | Same-interval replay validation completed for the configured source contract |
| Gold publication | Five documented Gold outputs produced |
| Master orchestration | ADF master pipeline coordinates source branches and gates Gold |
| Reconciliation | Reconciliation checks included in the completed workflow |

## Repository validation

The latest repository validation is [GitHub Actions run #830](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/45045827561), which completed successfully on `main` commit `231c949fae467eaec902b359be9d7baf1f693aac`. All workflow steps passed, including the complete repository test suite.

CI validates repository code, exported resource contracts and local/synthetic test scenarios. It does not provision or execute live Azure services; the Azure runtime acceptance described below is separately project-operator-confirmed.

## Latest enhancement deployment acceptance — 22 November 2026

The deployment operator confirms that the restored Azure deployment of the latest enhancement set ran without runtime errors. The following runtime outcomes were explicitly confirmed:

| Acceptance check | Result |
|---|---|
| Restored Azure deployment | Updated components ran without reported runtime errors |
| Gold pipeline | Completed successfully |
| Stable Gold consumer views | All five views contain data: `gold_order_360`, `gold_daily_order_performance`, `gold_payment_method_performance`, `gold_restaurant_feedback`, and `gold_driver_performance` |
| Gold quality metrics | Latest run metrics were persisted in `gold_quality_metrics` |
| Publication history | Successful publication was recorded in `gold_publication_history` |
| Run audit | Latest run was recorded in `gold_run_audit` |
| Active publication | Pointer was updated to the newly published Gold version |
| Retention maintenance | `Gold_Publication_Maintenance` completed with `dry_run=true` and identified cleanup candidates; no data was deleted |

The retention result is a dry-run acceptance only. Destructive retention deletes, `OPTIMIZE`, and `VACUUM` were not confirmed as executed and remain separately controlled maintenance actions.

These results are project-operator-confirmed and are retained in the dated [Azure owner-attestation record](run-evidence/2026-11-22-restored-azure-owner-attestation.md). Exact Azure resource identifiers, ADF/Databricks run IDs and URLs, timestamps, row counts, and raw notebook output exports were not supplied during the acceptance walkthrough. They are intentionally not fabricated; add sanitized originals when available using [Run Evidence and Retention](RUN_EVIDENCE_RETENTION.md).

## Technical boundaries

A successful end-to-end run validates the configured and tested source path. It does not remove the documented design limits of a strict lower watermark boundary, late-visible rows with equal/older cursor values, hard deletes without CDC/tombstones/soft deletes, or the absence of a null-checkpoint initial-load branch.

The completed project is not represented as a continuously hosted service and does not claim a production SLA, independent certification or universal exactly-once/change-capture guarantees.
