# Restored Azure acceptance — owner attestation

**Acceptance date:** 22 November 2026  
**Environment:** Restored Azure environment; private resource identifiers intentionally omitted  
**Evidence type:** Project-operator attestation recorded contemporaneously during the acceptance walkthrough  
**Repository release under test:** enhancement set merged to `main` at `231c949fae467eaec902b359be9d7baf1f693aac`  
**Repository CI reference:** [GitHub Actions run #830](https://github.com/viradidi/uber-eats-data-analytics-platform/actions/runs/45045827561) — repository validation only, not Azure runtime evidence

## Confirmed observations

The project operator reported that the restored components ran smoothly and no runtime errors occurred. The operator then confirmed the following acceptance checks:

| Component / check | Operator-reported observation |
|---|---|
| Restored deployment | Components ran without reported errors |
| Gold pipeline | Completed successfully |
| Stable Gold consumer views | All five views contained data: `gold_order_360`, `gold_daily_order_performance`, `gold_payment_method_performance`, `gold_restaurant_feedback`, and `gold_driver_performance` |
| Gold quality metrics | Latest-run records were present in `gold_quality_metrics` |
| Publication history | Successful publication was recorded in `gold_publication_history` |
| Run audit | Latest run was recorded in `gold_run_audit` |
| Active publication pointer | Updated to reference the newly published Gold version |
| Retention maintenance | `Gold_Publication_Maintenance` completed with `dry_run=true`; cleanup candidates were identified and no data was deleted |

## Evidence boundary

This file preserves the operator's explicit acceptance confirmation. It is **not** a substitute for raw Azure-generated evidence and does not claim independent inspection of the Azure subscription or Databricks workspace.

The following original artifacts were not supplied during this acceptance walkthrough and are therefore recorded as **not retained in this repository**:

- Azure Data Factory master and child pipeline run IDs, direct run URLs, timestamps, and activity output exports.
- Databricks notebook/job run IDs, direct run URLs, task output, and cluster/runtime metadata.
- Actual Silver/Gold row counts, reconciliation query output, and checkpoint before/after values.
- The Gold publication ID and the exact persisted pointer value.
- The retention dry-run output, candidate identifiers, protected-state reasons, and retention audit row.

No values have been estimated or fabricated. Add sanitized originals to this directory using [the runtime evidence template](runtime-evidence-template.md) when the source exports are available.

## Maintenance actions explicitly not accepted here

The acceptance described above covers a non-destructive retention dry run only. It does not confirm execution of destructive retention deletion, Delta `OPTIMIZE`, or `VACUUM`. Those actions remain separately governed and require their own approval and evidence.

Secrets, tokens, connection strings, personal data, customer/payment records, and sensitive Azure resource identifiers must not be committed.
