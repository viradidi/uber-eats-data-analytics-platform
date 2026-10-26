# SQL Incremental-Run Acceptance Queries

These are operator templates for validating a single SQL incremental extraction window. They are not plug-and-play queries: replace every `<placeholder>` with approved source-specific identifiers or values, and review the generated SQL before execution. Do not paste untrusted values into SQL.

Run these checks before advancing `pipeline_control.last_watermark_value`. The ADF export uses a strict lower bound and inclusive upper bound; the exact same bounds must be used for source and landing reconciliation.

## 1. Confirm the expected source interval

Replace the table, watermark column, lower watermark, upper watermark, and business key. Use literals formatted for the real SQL type and precision.

```sql
SELECT
    COUNT_BIG(*) AS expected_source_rows,
    COUNT_BIG(DISTINCT <business_key_column>) AS distinct_business_keys,
    SUM(CASE WHEN <business_key_column> IS NULL THEN 1 ELSE 0 END) AS null_business_keys,
    MIN(<watermark_column>) AS min_watermark_in_window,
    MAX(<watermark_column>) AS max_watermark_in_window
FROM dbo.<source_table>
WHERE <watermark_column> > <typed_lower_watermark>
  AND <watermark_column> <= <typed_upper_watermark>;
```

Use a composite-key expression or grouped key query instead of `COUNT_BIG(DISTINCT ...)` when the business key is composite. The source count is only a baseline; it does not prove the landing data contains the same rows.

## 2. Check for duplicate business keys in the landed batch

Point this query at the actual landing table or a temporary/staging table created from the landed files. Do not run it against the whole target if the target intentionally contains multiple versions per key.

```sql
SELECT <business_key_column>, COUNT_BIG(*) AS copies
FROM <landing_or_staging_object>
WHERE <run_or_window_predicate>
GROUP BY <business_key_column>
HAVING COUNT_BIG(*) > 1;
```

Any returned rows need an explicit explanation. If the source legitimately emits multiple events per business key, choose the event key (for example, business key plus event timestamp/sequence) rather than deduplicating valid history.

## 3. Compare source and landed counts for the same window

Record the source count from query 1 and compare it with the landed batch count for the exact logical run/window. Do not compare a full source table to a partial landing batch.

```sql
SELECT COUNT_BIG(*) AS landed_rows
FROM <landing_or_staging_object>
WHERE <run_or_window_predicate>;
```

A count match is necessary in many ingestion designs but is not sufficient: different rows can produce equal counts. Also compare stable business keys and important fields, or run a deterministic row-hash reconciliation where the serialization rules are documented.

## 4. Check the checkpoint gate

Before advancing the control watermark, retain evidence that:

- The source interval and its lower/upper bounds are recorded.
- The landed batch is associated with a stable run/window identifier.
- Expected and landed counts reconcile, and key/field reconciliation passes.
- Required Silver processing and its reconciliation succeeded.
- A rerun of the same logical interval follows a defined idempotency policy.
- The next checkpoint equals the successfully processed upper bound—not a newly sampled maximum beyond the extracted range.

If any gate fails, keep the checkpoint unchanged and follow `RECOVERY_AND_RERUN_RUNBOOK.md`. These templates do not create transactionality between SQL, ADLS, Databricks, and the control table; that must be provided by the orchestration and destination design.

## Evidence record

For each acceptance run, save the source/table contract, exact bounds, query outputs, run ID, landed location, duplicate-key results, reconciliation result, downstream job result, and checkpoint before/after. Runtime evidence must come from the environment where the pipeline is executed.
