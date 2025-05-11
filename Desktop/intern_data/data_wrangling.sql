USE droptime;

SELECT 
    COUNT(*) - COUNT(order_id) AS missing_order_id,
    COUNT(*) - COUNT(customer_id) AS missing_customer_id,
    COUNT(*) - COUNT(sector_id) AS missing_sector_id,
    COUNT(*) - COUNT(planned_delivery_duration) AS missing_planned_delivery_duration
FROM orders;

SELECT 
    COUNT(*) - COUNT(product_id) AS missing_product_id,
    COUNT(*) - COUNT(weight) AS missing_weight
FROM products;

SELECT 
    COUNT(*) - COUNT(order_id) AS missing_order_id,
    COUNT(*) - COUNT(product_id) AS missing_product_id,
    COUNT(*) - COUNT(quantity) AS missing_quantity
FROM orders_products;

SELECT 
    COUNT(*) - COUNT(segment_id) AS missing_segment_id,
    COUNT(*) - COUNT(driver_id) AS missing_driver_id,
    COUNT(*) - COUNT(order_id) AS missing_order_id,
    COUNT(*) - COUNT(segment_start_time) AS missing_segment_start_time,
    COUNT(*) - COUNT(segment_end_time) AS missing_segment_end_time
FROM route_segments;

SELECT 
    segment_type,
    COUNT(*) AS segment_count,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS missing_order_id_count
FROM route_segments
GROUP BY segment_type;

CREATE OR REPLACE VIEW clean_route_segments AS
SELECT *
FROM route_segments
WHERE segment_type = 'DRIVE'
   OR (segment_type = 'STOP' AND order_id IS NOT NULL);

SELECT 
  COUNT(*) - COUNT(planned_delivery_duration) AS missing_planned
FROM orders;

SELECT
    COUNT(*) AS total_rows,
    COUNT(planned_delivery_duration) AS non_nulls,
    MIN(planned_delivery_duration) AS min_val,
    MAX(planned_delivery_duration) AS max_val,
    ROUND(AVG(planned_delivery_duration), 2) AS avg_val,
    SUM(planned_delivery_duration = 0) AS zero_vals,
    SUM(planned_delivery_duration > 7200) AS over_2h_vals
FROM orders;

SELECT
    COUNT(*) AS total_rows,
    COUNT(weight) AS non_nulls,
    MIN(weight) AS min_val,
    MAX(weight) AS max_val,
    ROUND(AVG(weight), 2) AS avg_val,
    SUM(weight = 0) AS zero_vals
FROM products;

SELECT
    COUNT(*) AS total_rows,
    COUNT(quantity) AS non_nulls,
    MIN(quantity) AS min_val,
    MAX(quantity) AS max_val,
    ROUND(AVG(quantity), 2) AS avg_val,
    SUM(quantity = 0) AS zero_vals
FROM orders_products;


SELECT
    o.order_id,
    rs.segment_start_time,
    rs.segment_end_time,
    TIMESTAMPDIFF(SECOND, rs.segment_start_time, rs.segment_end_time) AS actual_delivery_duration
FROM orders o
JOIN clean_route_segments rs ON o.order_id = rs.order_id
WHERE rs.segment_type = 'STOP' 
AND rs.order_id IS NOT NULL
AND rs.segment_end_time < rs.segment_start_time;

CREATE OR REPLACE VIEW clean_route_segments_no_negatives AS
SELECT
    o.order_id,
    o.planned_delivery_duration,
    MIN(rs.segment_start_time) AS delivery_start,
    MAX(rs.segment_end_time) AS delivery_end,
    TIMESTAMPDIFF(SECOND, MIN(rs.segment_start_time), MAX(rs.segment_end_time)) AS actual_delivery_seconds
FROM orders o
JOIN clean_route_segments rs
    ON o.order_id = rs.order_id
WHERE rs.segment_type = 'STOP' 
  AND rs.order_id IS NOT NULL
  AND rs.segment_end_time > rs.segment_start_time  
GROUP BY o.order_id, o.planned_delivery_duration;

SELECT
    COUNT(*) AS total_rows,
    COUNT(planned_delivery_duration) AS non_nulls_planned,
    MIN(planned_delivery_duration) AS min_planned_duration,
    MAX(planned_delivery_duration) AS max_planned_duration,
    ROUND(AVG(planned_delivery_duration), 2) AS avg_planned_duration,
    SUM(planned_delivery_duration = 0) AS zero_planned_duration,
    COUNT(actual_delivery_seconds) AS non_nulls_actual,
    MIN(actual_delivery_seconds) AS min_actual_duration,
    MAX(actual_delivery_seconds) AS max_actual_duration,
    ROUND(AVG(actual_delivery_seconds), 2) AS avg_actual_duration,
    SUM(actual_delivery_seconds = 0) AS zero_actual_duration
FROM clean_route_segments_no_negatives;

WITH DeliveryTimes AS (
    SELECT 
        rs.order_id,
        CEIL(TIMESTAMPDIFF(SECOND, MIN(rs.delivery_start), MAX(rs.delivery_end)) / 60) AS actual_delivery_minutes
    FROM clean_route_segments_no_negatives rs
    JOIN orders o ON o.order_id = rs.order_id
    GROUP BY rs.order_id
)
SELECT 
    actual_delivery_minutes,
    COUNT(order_id) AS liczba_zamowien
FROM DeliveryTimes
GROUP BY actual_delivery_minutes
ORDER BY actual_delivery_minutes;

SELECT 
    o.order_id,
    o.planned_delivery_duration,
    rs.actual_delivery_seconds,
    (o.planned_delivery_duration - rs.actual_delivery_seconds) AS prediction_error
FROM clean_route_segments_no_negatives rs
JOIN orders o ON o.order_id = rs.order_id;

WITH PredictionErrors AS (
    SELECT
        o.order_id,
        (o.planned_delivery_duration - rs.actual_delivery_seconds) AS prediction_error
    FROM clean_route_segments_no_negatives rs
    JOIN orders o ON o.order_id = rs.order_id
)
SELECT
    prediction_error,
    COUNT(order_id) AS number_of_orders
FROM PredictionErrors
GROUP BY prediction_error
ORDER BY prediction_error;


SELECT 
    o.sector_id,
    AVG(rs.actual_delivery_seconds) AS avg_actual_delivery_seconds
FROM clean_route_segments_no_negatives rs
JOIN orders o ON o.order_id = rs.order_id
GROUP BY o.sector_id
ORDER BY avg_actual_delivery_seconds DESC;


SELECT 
    p.weight,
    AVG(rs.actual_delivery_seconds) AS avg_actual_delivery_seconds
FROM clean_route_segments_no_negatives rs
JOIN orders o ON o.order_id = rs.order_id
JOIN orders_products op ON op.order_id = o.order_id
JOIN products p ON p.product_id = op.product_id
GROUP BY p.weight
ORDER BY p.weight;

SELECT 
    o.order_id,
    o.planned_delivery_duration,
    rs.actual_delivery_seconds,
    (rs.actual_delivery_seconds - o.planned_delivery_duration) AS prediction_error
FROM clean_route_segments_no_negatives rs
JOIN orders o ON o.order_id = rs.order_id
ORDER BY prediction_error DESC;

SELECT 
    o.sector_id,
    AVG(rs.actual_delivery_seconds) AS avg_actual_delivery_seconds
FROM clean_route_segments_no_negatives rs
JOIN orders o ON o.order_id = rs.order_id
GROUP BY o.sector_id
ORDER BY avg_actual_delivery_seconds DESC;



-- 1. Delivery Time by Sector
SELECT
    o.sector_id,
    AVG(TIMESTAMPDIFF(SECOND, rs.segment_start_time, rs.segment_end_time)) AS avg_actual_delivery_seconds,
    COUNT(o.order_id) AS number_of_orders
FROM orders o
JOIN clean_route_segments rs ON o.order_id = rs.order_id
GROUP BY o.sector_id
ORDER BY avg_actual_delivery_seconds DESC;

-- 2. Delivery Time by Customer
SELECT
    o.customer_id,
    AVG(TIMESTAMPDIFF(SECOND, rs.segment_start_time, rs.segment_end_time)) AS avg_actual_delivery_seconds,
    COUNT(o.order_id) AS number_of_orders
FROM orders o
JOIN clean_route_segments rs ON o.order_id = rs.order_id
GROUP BY o.customer_id
ORDER BY avg_actual_delivery_seconds DESC;

-- 3. Delivery Time by Order Size (Number of Products)
SELECT
    op.order_id,
    COUNT(op.product_id) AS number_of_products,
    AVG(TIMESTAMPDIFF(SECOND, rs.segment_start_time, rs.segment_end_time)) AS avg_actual_delivery_seconds
FROM orders_products op
JOIN clean_route_segments rs ON op.order_id = rs.order_id
GROUP BY op.order_id
ORDER BY number_of_products;

-- 4. Delivery Time by Product Weight
SELECT
    p.weight,
    AVG(TIMESTAMPDIFF(SECOND, rs.segment_start_time, rs.segment_end_time)) AS avg_actual_delivery_seconds,
    COUNT(op.order_id) as number_of_orders
FROM products p
JOIN orders_products op ON p.product_id = op.product_id
JOIN clean_route_segments rs ON op.order_id = rs.order_id
GROUP BY p.weight
ORDER BY p.weight;

-- 5. Delivery Time by Planned Delivery Duration
SELECT
    o.planned_delivery_duration,
    AVG(TIMESTAMPDIFF(SECOND, rs.segment_start_time, rs.segment_end_time)) AS avg_actual_delivery_seconds,
        COUNT(o.order_id) AS number_of_orders
FROM orders o
JOIN clean_route_segments rs ON o.order_id = rs.order_id
GROUP BY o.planned_delivery_duration
ORDER BY o.planned_delivery_duration;