-- Operational audit queries for the Uber Eats Silver quality tables.
-- Run in Databricks SQL after the corresponding Delta tables have been created.
-- These are read-only diagnostics; they do not mutate pipeline or target data.

-- 1. Latest recorded quality outcome per dataset.
WITH ranked_quality AS (
    SELECT
        run_id,
        dataset_name,
        source_rows,
        prepared_rows,
        target_rows_after,
        status,
        recorded_at_utc,
        ROW_NUMBER() OVER (
            PARTITION BY dataset_name
            ORDER BY recorded_at_utc DESC, run_id DESC
        ) AS row_num
    FROM ubereats_databricks1.silver.pipeline_quality_results
)
SELECT
    dataset_name,
    run_id,
    status,
    source_rows,
    prepared_rows,
    target_rows_after,
    recorded_at_utc
FROM ranked_quality
WHERE row_num = 1
ORDER BY dataset_name;


-- 2. Recent reconciliation failures, including the checks that failed.
SELECT
    run_id,
    dataset_name,
    target_table,
    check_type,
    metric_name,
    source_value,
    target_value,
    difference,
    recorded_at_utc
FROM ubereats_databricks1.silver.pipeline_reconciliation_details
WHERE status = 'FAILED_RECONCILIATION'
ORDER BY recorded_at_utc DESC
LIMIT 200;


-- 3. Failure events correlated with reconciliation details by run ID.
-- LEFT JOIN retains failures even when no detailed reconciliation row exists.
SELECT
    f.recorded_at_utc AS failure_recorded_at_utc,
    f.run_id,
    f.dataset_name,
    f.stage,
    f.error_type,
    f.error_message,
    d.check_type,
    d.metric_name,
    d.source_value,
    d.target_value,
    d.difference,
    d.status AS reconciliation_status
FROM ubereats_databricks1.silver.pipeline_failure_events f
LEFT JOIN ubereats_databricks1.silver.pipeline_reconciliation_details d
    ON f.run_id = d.run_id
   AND f.dataset_name = d.dataset_name
WHERE f.stage IN (
    'silver_reconciliation',
    'snapshot_reconciliation_precondition',
    'full_snapshot_reconciliation'
)
ORDER BY f.recorded_at_utc DESC
LIMIT 200;


-- 4. Rejected-row volume by dataset and reason.
SELECT
    dataset_name,
    rejection_reason,
    COUNT(*) AS rejected_rows,
    MAX(quarantined_at_utc) AS most_recent_rejection_utc
FROM ubereats_databricks1.silver.pipeline_rejected_records
GROUP BY dataset_name, rejection_reason
ORDER BY rejected_rows DESC, dataset_name, rejection_reason;


-- 5. Recent merge-based reconciliation outcomes.
SELECT
    run_id,
    dataset_name,
    target_table,
    source_rows,
    missing_target_keys,
    mismatched_rows,
    status,
    recorded_at_utc
FROM ubereats_databricks1.silver.pipeline_reconciliation_results
ORDER BY recorded_at_utc DESC
LIMIT 200;
