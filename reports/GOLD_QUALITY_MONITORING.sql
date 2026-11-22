-- Gold quality and anomaly monitoring report.
-- Default table: ubereats_databricks1.gold.gold_quality_metrics
-- Replace the catalog/schema when using another restored environment.
-- Read-only queries; no data is modified.

-- 1) Recent anomaly observations with their historical context.
SELECT
    recorded_at_utc,
    pipeline_run_id,
    run_id,
    metric_name,
    dimension_key,
    metric_unit,
    metric_value,
    baseline_mean,
    baseline_stddev,
    baseline_observation_count,
    deviation_ratio,
    deviation_z_score,
    status
FROM ubereats_databricks1.gold.gold_quality_metrics
WHERE status = 'ANOMALY'
  AND recorded_at_utc >= current_timestamp() - INTERVAL 30 DAYS
ORDER BY recorded_at_utc DESC, metric_name, dimension_key;


-- 2) Daily quality-status trend by metric.
SELECT
    CAST(recorded_at_utc AS DATE) AS run_date,
    metric_name,
    status,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT run_id) AS gold_run_count,
    AVG(metric_value) AS average_metric_value,
    MAX(recorded_at_utc) AS latest_observation_utc
FROM ubereats_databricks1.gold.gold_quality_metrics
WHERE recorded_at_utc >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY CAST(recorded_at_utc AS DATE), metric_name, status
ORDER BY run_date DESC, metric_name, status;


-- 3) Latest status for every metric/business dimension.
WITH ranked_observations AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY metric_name, dimension_key
            ORDER BY recorded_at_utc DESC, run_id DESC
        ) AS observation_rank
    FROM ubereats_databricks1.gold.gold_quality_metrics
)
SELECT
    metric_name,
    dimension_key,
    metric_unit,
    metric_value,
    baseline_mean,
    deviation_ratio,
    deviation_z_score,
    baseline_observation_count,
    status,
    recorded_at_utc,
    pipeline_run_id
FROM ranked_observations
WHERE observation_rank = 1
ORDER BY
    CASE status
        WHEN 'ANOMALY' THEN 1
        WHEN 'INSUFFICIENT_HISTORY' THEN 2
        ELSE 3
    END,
    metric_name,
    dimension_key;
