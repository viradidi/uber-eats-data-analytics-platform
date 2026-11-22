-- Gold atomic-publication operational queries.
-- Read-only sections first; use the commented rollback template only after
-- change approval and verification of all five version tables.

-- 1) Current Gold publication pointer.
SELECT
    publication_key,
    active_publication_id,
    updated_at_utc,
    pipeline_run_id
FROM ubereats_databricks1.gold.gold_publication_state
WHERE publication_key = 'gold_analytics';


-- 2) Latest publication lifecycle events.
SELECT
    recorded_at_utc,
    publication_id,
    pipeline_run_id,
    event_status,
    previous_publication_id,
    output_counts_json,
    error_message
FROM ubereats_databricks1.gold.gold_publication_history
ORDER BY recorded_at_utc DESC
LIMIT 100;


-- 3) Count active and inactive version rows by output.
WITH version_counts AS (
    SELECT 'gold_order_360' AS output_name, _publication_id, COUNT(*) AS row_count
    FROM ubereats_databricks1.gold.gold_order_360__versions
    GROUP BY _publication_id
    UNION ALL
    SELECT 'gold_daily_order_performance', _publication_id, COUNT(*)
    FROM ubereats_databricks1.gold.gold_daily_order_performance__versions
    GROUP BY _publication_id
    UNION ALL
    SELECT 'gold_payment_method_performance', _publication_id, COUNT(*)
    FROM ubereats_databricks1.gold.gold_payment_method_performance__versions
    GROUP BY _publication_id
    UNION ALL
    SELECT 'gold_restaurant_feedback', _publication_id, COUNT(*)
    FROM ubereats_databricks1.gold.gold_restaurant_feedback__versions
    GROUP BY _publication_id
    UNION ALL
    SELECT 'gold_driver_performance', _publication_id, COUNT(*)
    FROM ubereats_databricks1.gold.gold_driver_performance__versions
    GROUP BY _publication_id
)
SELECT
    v.output_name,
    v._publication_id AS publication_id,
    v.row_count,
    CASE WHEN v._publication_id = s.active_publication_id THEN 'ACTIVE' ELSE 'INACTIVE' END AS visibility
FROM version_counts v
CROSS JOIN ubereats_databricks1.gold.gold_publication_state s
WHERE s.publication_key = 'gold_analytics'
ORDER BY visibility, publication_id, output_name;


-- 4) ROLLBACK TEMPLATE — intentionally commented; review before execution.
-- Replace <EXPECTED_ACTIVE_PUBLICATION_ID> with the current pointer value and
-- <TARGET_PUBLICATION_ID> with a previously successful publication ID. First
-- verify that all five version tables contain the target publication ID.
--
-- MERGE INTO ubereats_databricks1.gold.gold_publication_state AS target
-- USING (
--     SELECT
--         'gold_analytics' AS publication_key,
--         '<EXPECTED_ACTIVE_PUBLICATION_ID>' AS expected_active_publication_id,
--         '<TARGET_PUBLICATION_ID>' AS target_publication_id,
--         'approved-manual-rollback' AS pipeline_run_id
-- ) AS source
-- ON target.publication_key = source.publication_key
-- WHEN MATCHED
--  AND target.active_publication_id <=> source.expected_active_publication_id
-- THEN UPDATE SET
--     target.active_publication_id = source.target_publication_id,
--     target.updated_at_utc = current_timestamp(),
--     target.pipeline_run_id = source.pipeline_run_id;


-- 5) Retention-run audit: dry runs and executed cleanup outcomes.
SELECT
    recorded_at_utc,
    retention_run_id,
    dry_run,
    retention_days,
    keep_successful_publications,
    active_publication_id,
    candidate_publication_ids_json,
    inventory_publication_count,
    row_counts_json,
    optimize_requested,
    vacuum_requested,
    vacuum_retention_hours,
    status,
    error_message
FROM ubereats_databricks1.gold.gold_publication_retention_runs
ORDER BY recorded_at_utc DESC
LIMIT 100;
