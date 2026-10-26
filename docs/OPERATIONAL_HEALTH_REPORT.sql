-- Dataset-level operational health report for the Uber Eats Silver layer.
-- Run in Databricks SQL after the quality, reconciliation, failure, and quarantine
-- Delta tables have been created by the pipeline.
-- Read-only: this query does not mutate data or infer live execution status.
--
-- Interpretation:
--   latest_quality_status: latest recorded Silver publication outcome.
--   latest_merge_reconciliation_status: latest merge-target reconciliation outcome.
--   failures_last_24h: failure events recorded in the last 24 hours.
--   rejected_rows_last_24h: rejected records quarantined in the last 24 hours.
-- A NULL status means no record exists for that dataset/table; it does not mean success.

WITH quality_ranked AS (
    SELECT
        dataset_name,
        run_id,
        status,
        source_rows,
        prepared_rows,
        target_rows_after,
        recorded_at_utc,
        ROW_NUMBER() OVER (
            PARTITION BY dataset_name
            ORDER BY recorded_at_utc DESC, run_id DESC
        ) AS row_num
    FROM ubereats_databricks1.silver.pipeline_quality_results
),
latest_quality AS (
    SELECT
        dataset_name,
        run_id AS latest_quality_run_id,
        status AS latest_quality_status,
        source_rows AS latest_source_rows,
        prepared_rows AS latest_prepared_rows,
        target_rows_after AS latest_target_rows,
        recorded_at_utc AS latest_quality_at_utc
    FROM quality_ranked
    WHERE row_num = 1
),
reconciliation_ranked AS (
    SELECT
        dataset_name,
        run_id,
        target_table,
        source_rows,
        missing_target_keys,
        mismatched_rows,
        status,
        recorded_at_utc,
        ROW_NUMBER() OVER (
            PARTITION BY dataset_name
            ORDER BY recorded_at_utc DESC, run_id DESC
        ) AS row_num
    FROM ubereats_databricks1.silver.pipeline_reconciliation_results
),
latest_reconciliation AS (
    SELECT
        dataset_name,
        run_id AS latest_reconciliation_run_id,
        target_table AS latest_reconciliation_target,
        status AS latest_merge_reconciliation_status,
        source_rows AS latest_reconciliation_source_rows,
        missing_target_keys AS latest_missing_target_keys,
        mismatched_rows AS latest_mismatched_rows,
        recorded_at_utc AS latest_reconciliation_at_utc
    FROM reconciliation_ranked
    WHERE row_num = 1
),
failure_24h AS (
    SELECT
        dataset_name,
        COUNT(*) AS failures_last_24h,
        MAX(recorded_at_utc) AS latest_failure_at_utc
    FROM ubereats_databricks1.silver.pipeline_failure_events
    WHERE recorded_at_utc >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY dataset_name
),
rejected_24h AS (
    SELECT
        dataset_name,
        COUNT(*) AS rejected_rows_last_24h,
        MAX(quarantined_at_utc) AS latest_rejection_at_utc
    FROM ubereats_databricks1.silver.pipeline_rejected_records
    WHERE quarantined_at_utc >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY dataset_name
),
dataset_inventory AS (
    SELECT dataset_name FROM latest_quality
    UNION
    SELECT dataset_name FROM latest_reconciliation
    UNION
    SELECT dataset_name FROM failure_24h
    UNION
    SELECT dataset_name FROM rejected_24h
)
SELECT
    i.dataset_name,
    q.latest_quality_status,
    q.latest_quality_run_id,
    q.latest_source_rows,
    q.latest_prepared_rows,
    q.latest_target_rows,
    q.latest_quality_at_utc,
    r.latest_merge_reconciliation_status,
    r.latest_reconciliation_run_id,
    r.latest_reconciliation_target,
    r.latest_reconciliation_source_rows,
    r.latest_missing_target_keys,
    r.latest_mismatched_rows,
    r.latest_reconciliation_at_utc,
    COALESCE(f.failures_last_24h, 0) AS failures_last_24h,
    f.latest_failure_at_utc,
    COALESCE(x.rejected_rows_last_24h, 0) AS rejected_rows_last_24h,
    x.latest_rejection_at_utc,
    CASE
        WHEN q.latest_quality_status = 'FAILED_RECONCILIATION'
          OR r.latest_merge_reconciliation_status = 'FAILED_RECONCILIATION'
          OR COALESCE(f.failures_last_24h, 0) > 0
            THEN 'INVESTIGATE'
        WHEN q.latest_quality_status IS NULL
          AND r.latest_merge_reconciliation_status IS NULL
            THEN 'NO_SUCCESS_OR_RECONCILIATION_RECORD'
        WHEN q.latest_quality_status = 'SUCCEEDED'
          AND (
              r.latest_merge_reconciliation_status IS NULL
              OR r.latest_merge_reconciliation_status = 'SUCCEEDED'
          )
            THEN 'NO_RECENT_FAILURE_SIGNAL'
        ELSE 'REVIEW_STATUS'
    END AS operational_signal
FROM dataset_inventory i
LEFT JOIN latest_quality q
    ON i.dataset_name = q.dataset_name
LEFT JOIN latest_reconciliation r
    ON i.dataset_name = r.dataset_name
LEFT JOIN failure_24h f
    ON i.dataset_name = f.dataset_name
LEFT JOIN rejected_24h x
    ON i.dataset_name = x.dataset_name
ORDER BY
    CASE
        WHEN q.latest_quality_status = 'FAILED_RECONCILIATION'
          OR r.latest_merge_reconciliation_status = 'FAILED_RECONCILIATION'
          OR COALESCE(f.failures_last_24h, 0) > 0 THEN 0
        WHEN q.latest_quality_status IS NULL
          AND r.latest_merge_reconciliation_status IS NULL THEN 1
        ELSE 2
    END,
    i.dataset_name;
