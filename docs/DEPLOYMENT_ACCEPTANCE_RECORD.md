# Azure deployment and end-to-end execution record

## Completion status

**Completed — successfully deployed and executed end to end in Azure.**

The completed Uber Eats platform covers API, CSV and SQL ingestion; Bronze and Silver processing; the repository-owned SQL Silver path; incremental replay validation; Gold publication; the master ADF orchestration; and reconciliation.

The five documented Gold outputs are:

- `gold_order_360`
- `gold_daily_order_performance`
- `gold_payment_method_performance`
- `gold_restaurant_feedback`
- `gold_driver_performance`

The Azure resources used for the completed run were later decommissioned to control ongoing costs. The repository preserves the source code, ADF definitions, Databricks notebooks, SQL, tests, architecture and operational documentation needed to understand and redeploy the platform.

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

The latest repository test run is [GitHub Actions run #779](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/42021275338), which passed **135 tests** on commit `034a19d5d62a94bb75e510f03e214f59590bf54a`.

CI validates repository code, exported resource contracts and local/synthetic test scenarios. It does not provision or execute live Azure services; the Azure workflow was separately deployed and executed end to end.

## Technical boundaries

A successful end-to-end run validates the configured and tested source path. It does not remove the documented design limits of a strict lower watermark boundary, late-visible rows with equal/older cursor values, hard deletes without CDC/tombstones/soft deletes, or the absence of a null-checkpoint initial-load branch.

The completed project is not represented as a continuously hosted service and does not claim a production SLA, independent certification or universal exactly-once/change-capture guarantees.


## Restored-environment verification — 22 November 2026

The project owner reports that the restored Azure components ran smoothly without errors. The following additional checks were explicitly confirmed during the acceptance walkthrough:

| Check | Owner-reported result |
|---|---|
| Restored components / pipeline execution | Completed without reported errors |
| Five Gold consumer views | All five contain data |
| `gold_quality_metrics` | Latest-run quality metrics present |
| `gold_publication_history` | Successful publication recorded |
| `gold_run_audit` | Run audit recorded |
| Active Gold publication pointer | References the newly published version |
| Gold retention maintenance | Dry run completed successfully; no data was deleted |

The latest main-branch CI validation also passed: [GitHub Actions run #830](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/45045827561), on commit `231c949fae467eaec902b359be9d7baf1f693aac`.

**Evidence qualification:** These restored-environment outcomes are recorded from the project owner's confirmation in the acceptance walkthrough. Exact Azure run IDs, timestamps, row counts, query outputs, and exported logs/screenshots were not supplied for attachment to this record. The record therefore does not fabricate those artifacts or claim independent inspection of the live Azure workspace. The retention result confirms a non-destructive dry run only; no deletion execution was performed or approved.
