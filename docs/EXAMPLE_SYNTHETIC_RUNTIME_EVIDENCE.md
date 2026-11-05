# Synthetic Runtime Evidence Example (Illustrative Only)

> **IMPORTANT — NOT REAL AZURE EXECUTION EVIDENCE.**
>
> Every run identifier, timestamp, duration, row count, audit value, and status detail in this document is fabricated for documentation/demo purposes. These values were not retrieved from Azure Data Factory, Azure Databricks, Azure SQL, or the deployed environment. Do not cite, present, or upload this example as proof of an actual run. Replace the entire sample with captured real execution details when available.

## Purpose

This file demonstrates a concise format for retaining runtime evidence for the Uber Eats Data Analytics Platform. The owner has reported successful Azure deployment and verified end-to-end operation; this sample does not independently establish that operation.

## Example run summary — fabricated

| Field | Illustrative value |
|---|---|
| Evidence label | SYNTHETIC / DEMO |
| ADF master pipeline | `pl_master_uber_eats` |
| ADF run ID | `SIM-ADF-MASTER-0001` |
| Databricks workflow/run reference | `SIM-DBX-GOLD-0001` |
| Start time | `2026-11-05T08:00:00+03:00` (fabricated) |
| End time | `2026-11-05T08:24:00+03:00` (fabricated) |
| Overall result | `SUCCESS (SIMULATED)` |
| Source/target reconciliation | `PASS (SIMULATED)` |

## Example activity evidence — fabricated

| Activity | Illustrative result | Illustrative rows |
|---|---|---:|
| API ingestion → Bronze | SUCCESS (SIMULATED) | 12,450 |
| CSV ingestion → Bronze | SUCCESS (SIMULATED) | 3,200 |
| SQL ingestion → Bronze | SUCCESS (SIMULATED) | 8,750 |
| Silver transformations | SUCCESS (SIMULATED) | 24,100 |
| SQL Silver incremental MERGE | SUCCESS (SIMULATED) | 8,750 |
| Gold analytics build | SUCCESS (SIMULATED) | 5,600 |
| Reconciliation / audit checks | PASS (SIMULATED) | 5 Gold tables |

The counts above are arbitrary example values. They are not known project outputs and must not be used as benchmark or acceptance numbers.

## Replace this example with real evidence

For each actual accepted execution, capture:

1. The ADF master pipeline run ID and portal URL.
2. Actual start/end timestamps and final status.
3. Child pipeline and activity run IDs/statuses.
4. Databricks notebook/job run IDs and URLs.
5. Actual source, Bronze, Silver and Gold counts for the same run/window.
6. Replay validation results (including the second execution and duplicate/reconciliation checks).
7. Actual audit/reconciliation outputs and any failures/resolution notes.
8. A sanitized export or screenshot; remove secrets, tokens, personal data, and sensitive endpoint details.

## Evidence integrity rule

Keep this document clearly labeled as synthetic until replaced by real run records. The acceptance status in project documentation is owner-confirmed; raw logs and IDs should only be called “retained evidence” after actual records are attached.
