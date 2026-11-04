-- Uber Eats Gold analytics reporting pack.
-- Run after pipeline/uber_eats_master has published the Gold Delta tables.
-- All queries are read-only and use synthetic project data.

-- 1. Daily order and fulfilment KPIs.
SELECT
    order_date,
    order_count,
    delivered_orders,
    cancelled_orders,
    active_orders,
    paid_orders,
    successful_payment_amount,
    average_delivery_minutes,
    average_customer_rating
FROM ubereats_databricks1.gold.gold_daily_order_performance
ORDER BY order_date DESC;

-- 2. Current order-state distribution by order date.
SELECT
    order_date,
    current_order_status,
    COUNT(*) AS order_count,
    SUM(successful_payment_amount) AS successful_payment_amount
FROM ubereats_databricks1.gold.gold_order_360
GROUP BY order_date, current_order_status
ORDER BY order_date DESC, order_count DESC;

-- 3. Payment method and outcome performance.
SELECT
    payment_date,
    payment_method,
    payment_status,
    transaction_count,
    distinct_order_count,
    gross_attempt_amount,
    successful_payment_amount
FROM ubereats_databricks1.gold.gold_payment_method_performance
ORDER BY payment_date DESC, transaction_count DESC;

-- 4. Driver delivery performance; completion rate is delivered records / all records.
SELECT
    driver_id,
    driver_name,
    city,
    vehicle_type,
    delivery_record_count,
    distinct_order_count,
    delivered_record_count,
    cancelled_record_count,
    average_delivery_minutes,
    delivery_completion_rate
FROM ubereats_databricks1.gold.gold_driver_performance
ORDER BY delivery_completion_rate DESC, delivered_record_count DESC;

-- 5. Restaurant feedback ranking. Consider rating_count when comparing averages.
SELECT
    restaurant_id,
    rated_order_count,
    rating_count,
    average_rating,
    five_star_rating_count,
    five_star_rating_share
FROM ubereats_databricks1.gold.gold_restaurant_feedback
ORDER BY average_rating DESC, rating_count DESC;

-- 6. Order-level drill-through for operational investigation.
SELECT *
FROM ubereats_databricks1.gold.gold_order_360
ORDER BY order_created_at DESC
LIMIT 100;
