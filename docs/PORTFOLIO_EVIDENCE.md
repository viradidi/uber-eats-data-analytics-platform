# Portfolio Evidence Pack

## Purpose

This guide lists the evidence that makes the project's reported end-to-end validation easy for a hiring manager or reviewer to assess. The implementation owner reports that the workflows and checks have been validated successfully. Evidence should make that validation inspectable without overstating what a screenshot, unit test, or exported configuration proves.

**Do not redeploy Azure resources just to produce portfolio evidence.** Use existing retained run history, logs, test output, screenshots, and sanitized result files. If an artifact was not retained, mark it unavailable rather than fabricating it.

## Evidence to retain or link

| Priority | Evidence | What it should show | Suggested location |
|---|---|---|---|
| 1 — Required | Automated test result | A green GitHub Actions run or saved test output showing the exact workflow/command, result, and date/commit | Link the successful [Actions run](https://github.com/viradidi/uber-eats-data-analytics-platform/actions) from the README or add a sanitized capture under `docs/evidence/` |
| 1 — Required | Successful orchestration run | ADF pipeline name, run ID, start/end time, final status, and the key activities that completed | `docs/evidence/adf-successful-run.png` or a short sanitized run record |
| 1 — Required | Successful transformation run | Databricks job/notebook name, run ID, final status, target layer/table, and processed-row summary where available | `docs/evidence/databricks-successful-run.png` or a sanitized run record |
| 1 — Required | Reconciliation result | Source-versus-target counts or agreed reconciliation checks, plus the pass/fail outcome; include the relevant query or output | Sanitized output under `docs/evidence/`, paired with the existing reconciliation SQL |
| 2 — Strongly recommended | Monitoring / data-quality view | Real metrics or alerts such as run status, failed/rejected rows, freshness, row counts, or quality trends, with the time window visible | `docs/evidence/monitoring-dashboard.png` |
| 2 — Strongly recommended | Recovery / rerun proof | The failure or controlled test condition, the recovery action, the rerun outcome, and evidence that replay did not create unintended duplicates or advance state prematurely | `docs/evidence/recovery-rerun.md` with redacted log excerpts or screenshots |
| 2 — Strongly recommended | Incremental-load proof | Initial load followed by a second run with unchanged data and/or a changed source record; show expected inserted/updated counts and checkpoint behavior | `docs/evidence/incremental-load.md` |
| 3 — Useful | Data-output sample | Small, sanitized Bronze/Silver or curated output sample showing schema and representative transformed rows | `docs/evidence/sample-output.csv` or `.json` |
| 3 — Useful | Deployment/configuration proof | Relevant exported resource names and parameterized configuration; redact connection details and secrets | Link to the committed `pipeline/`, `dataset/`, `linkedService/`, and `notebooks/` assets |

## Minimum evidence pack

If only a few artifacts can be included, prioritize these four:

1. **Green automated validation** — a direct link to the exact successful GitHub Actions run, not only a badge.
2. **Successful ADF run** — the pipeline run details and status.
3. **Successful Databricks run** — the job/notebook run details and status.
4. **Reconciliation result** — the actual outcome from the relevant reconciliation checks.

The repository already contains implementation artifacts and engineering documentation. These runtime artifacts complement that material; they do not replace code review or explain every design decision.

## Evidence record template

Copy this template for each important validation scenario and fill it with actual values:

- **Scenario:** 
- **Environment:** local / test / Azure / Databricks (specify actual environment)
- **Date and time (with timezone):** 
- **Commit or version:** 
- **Pipeline / job / test command:** 
- **Run ID or workflow URL:** 
- **Expected outcome:** 
- **Observed outcome:** 
- **Rows read / written / updated / rejected (if applicable):** 
- **Reconciliation result:** 
- **Recovery or checkpoint behavior observed (if applicable):** 
- **Evidence file or link:** 
- **Limitations / notes:** 

## Capture and redaction rules

- Show the actual run status and relevant context; avoid cropped screenshots that hide whether the run succeeded.
- Include the run ID, workflow URL, timestamp, or commit where available so the evidence can be traced.
- Redact tokens, API keys, passwords, connection strings, tenant/subscription identifiers if private, customer data, and any personal or sensitive records.
- Use synthetic or approved sample rows only. Never publish real customer or payment data.
- Prefer a direct link to the authoritative GitHub Actions run when possible; screenshots are supporting context, not a substitute for the underlying run.
- Distinguish **repository tests** from **live service execution**. A green unit-test run does not prove that ADF or Databricks executed, and an exported JSON definition does not prove a deployed resource is running.
- Do not describe a failure/recovery test as passed unless its expected outcome and observed result were recorded.
- If Azure resources have been removed to control cost, retain previously captured evidence if available; do not recreate resources solely for a screenshot.

## Reviewer interpretation

The strongest evidence chain is:

1. A source change or controlled input is identified.
2. The orchestration and transformation runs complete.
3. Data-quality and reconciliation checks report the expected outcome.
4. Monitoring or run logs expose the result.
5. A rerun/recovery scenario demonstrates the documented behavior.

Use only the steps that were actually exercised. The project owner reports successful validation; this checklist identifies the artifacts that make that validation easier for an external reviewer to verify.
