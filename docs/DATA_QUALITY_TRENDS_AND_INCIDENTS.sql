-- Data-quality trend and incident-analysis report for the Uber Eats Silver layer.
-- Read-only Databricks SQL. Requires the four audit Delta tables to exist.
-- Window: the current date plus the preceding seven calendar days (eight dates total).
--
-- Each row is one dataset/day. Missing event categories are shown as zero.
-- Rejected-row trends count quarantine records, not unique source entities.
-- Failure counts are event counts, not incident counts or incident duration.
-- A quality/reconciliation status of FAILED_RECONCILIATION is counted as a
-- failed recorded outcome; the report does not infer unrecorded failures.

WITH quality_daily AS (
    SELECT
        dataset_name,
        CAST(TRY_CAST(recorded_at_utc AS TIMESTAMP) AS DATE) AS event_date,
        COUNT(*) AS quality_runs,
        SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS quality_successes,
        SUM(CASE WHEN status <> 'SUCCEEDED' OR status IS NULL THEN 1 ELSE 0 END) AS quality_non_successes,
        MAX(TRY_CAST(recorded_at_utc AS TIMESTAMP)) AS latest_quality_event_utc
    FROM ubereats_databricks1.silver.pipeline_quality_results
    WHERE CAST(TRY_CAST(recorded_at_utc AS TIMESTAMP) AS DATE) >= date_sub(current_date(), 7)
    GROUP BY dataset_name, CAST(TRY_CAST(recorded_at_utc AS TIMESTAMP) AS DATE)
),
reconciliation_daily AS (
    SELECT
        dataset_name,
        CAST(TRY_CAST(recorded_at_utc AS TIMESTAMP) AS DATE) AS event_date,
        COUNT(*) AS reconciliation_runs,
        SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS reconciliation_successes,
        SUM(CASE WHEN status <> 'SUCCEEDED' OR status IS NULL THEN 1 ELSE 0 END) AS reconciliation_non_successes,
        SUM(COALESCE(missing_target_keys, 0)) AS missing_target_keys_total,
        SUM(COALESCE(mismatched_rows, 0)) AS mismatched_rows_total,
        MAX(TRY_CAST(recorded_at_utc AS TIMESTAMP)) AS latest_reconciliation_event_utc
    FROM ubereats_databricks1.silver.pipeline_reconciliation_results
    WHERE CAST(TRY_CAST(recorded_at_utc AS TIMESTAMP) AS DATE) >= date_sub(current_date(), 7)
    GROUP BY dataset_name, CAST(TRY_CAST(recorded_at_utc AS TIMESTAMP) AS DATE)
),
failure_daily AS (
    SELECT
        dataset_name,
        CAST(TRY_CAST(recorded_at_utc AS TIMESTAMP) AS DATE) AS event_date,
        COUNT(*) AS failure_events,
        MAX(TRY_CAST(recorded_at_utc AS TIMESTAMP)) AS latest_failure_event_utc
    FROM ubereats_databricks1.silver.pipeline_failure_events
    WHERE CAST(TRY_CAST(recorded_at_utc AS TIMESTAMP) AS DATE) >= date_sub(current_date(), 7)
    GROUP BY dataset_name, CAST(TRY_CAST(recorded_at_utc AS TIMESTAMP) AS DATE)
),
rejection_daily AS (
    SELECT
        dataset_name,
        CAST(TRY_CAST(quarantined_at_utc AS TIMESTAMP) AS DATE) AS event_date,
        COUNT(*) AS rejected_rows,
        MAX(TRY_CAST(quarantined_at_utc AS TIMESTAMP)) AS latest_rejection_utc
    FROM ubereats_databricks1.silver.pipeline_rejected_records
    WHERE CAST(TRY_CAST(quarantined_at_utc AS TIMESTAMP) AS DATE) >= date_sub(current_date(), 7)
    GROUP BY dataset_name, CAST(TRY_CAST(quarantined_at_utc AS TIMESTAMP) AS DATE)
),
dataset_inventory AS (
    SELECT dataset_name FROM quality_daily
    UNION
    SELECT dataset_name FROM reconciliation_daily
    UNION
    SELECT dataset_name FROM failure_daily
    UNION
    SELECT dataset_name FROM rejection_daily
),
calendar AS (
    SELECT EXPLODE(
        SEQUENCE(date_sub(current_date(), 7), current_date(), INTERVAL 1 DAY)
    ) AS event_date
),
daily_keys AS (
    SELECT i.dataset_name, c.event_date
    FROM dataset_inventory i
    CROSS JOIN calendar c
),
daily_combined AS (
    SELECT
        k.dataset_name,
        k.event_date,
        COALESCE(q.quality_runs, 0) AS quality_runs,
        COALESCE(q.quality_successes, 0) AS quality_successes,
        COALESCE(q.quality_non_successes, 0) AS quality_non_successes,
        COALESCE(r.reconciliation_runs, 0) AS reconciliation_runs,
        COALESCE(r.reconciliation_successes, 0) AS reconciliation_successes,
        COALESCE(r.reconciliation_non_successes, 0) AS reconciliation_non_successes,
        COALESCE(r.missing_target_keys_total, 0) AS missing_target_keys_total,
        COALESCE(r.mismatched_rows_total, 0) AS mismatched_rows_total,
        COALESCE(f.failure_events, 0) AS failure_events,
        COALESCE(x.rejected_rows, 0) AS rejected_rows,
        GREATEST(
            q.latest_quality_event_utc,
            r.latest_reconciliation_event_utc,
            f.latest_failure_event_utc,
            x.latest_rejection_utc
        ) AS latest_recorded_event_utc
    FROM daily_keys k
    LEFT JOIN quality_daily q
        ON k.dataset_name = q.dataset_name AND k.event_date = q.event_date
    LEFT JOIN reconciliation_daily r
        ON k.dataset_name = r.dataset_name AND k.event_date = r.event_date
    LEFT JOIN failure_daily f
        ON k.dataset_name = f.dataset_name AND k.event_date = f.event_date
    LEFT JOIN rejection_daily x
        ON k.dataset_name = x.dataset_name AND k.event_date = x.event_date
),
with_deltas AS (
    SELECT
        *,
        LAG(rejected_rows) OVER (
            PARTITION BY dataset_name ORDER BY event_date
        ) AS prior_day_rejected_rows,
        LAG(failure_events) OVER (
            PARTITION BY dataset_name ORDER BY event_date
        ) AS prior_day_failure_events
    FROM daily_combined
)
SELECT
    dataset_name,
    event_date,
    quality_runs,
    quality_successes,
    quality_non_successes,
    reconciliation_runs,
    reconciliation_successes,
    reconciliation_non_successes,
    missing_target_keys_total,
    mismatched_rows_total,
    failure_events,
    prior_day_failure_events,
    rejected_rows,
    prior_day_rejected_rows,
    CASE
        WHEN prior_day_rejected_rows IS NULL THEN NULL
        ELSE rejected_rows - prior_day_rejected_rows
    END AS rejected_rows_day_over_day_change,
    latest_recorded_event_utc,
    CASE
        WHEN quality_non_successes > 0
          OR reconciliation_non_successes > 0
          OR failure_events > 0
            THEN 'INVESTIGATE_FAILURE_OR_RECONCILIATION'
        WHEN rejected_rows > 0
            THEN 'REVIEW_REJECTED_DATA'
        ELSE 'NO_RECORDED_FAILURE_SIGNAL'
    END AS daily_operational_signal
FROM with_deltas
ORDER BY event_date DESC, daily_operational_signal, dataset_name;
