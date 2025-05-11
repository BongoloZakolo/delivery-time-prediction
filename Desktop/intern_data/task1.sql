USE droptime;

SELECT
    op.product_id AS productID,
    SUM(p.weight) AS totalWeight
FROM orders AS o
JOIN orders_products AS op
ON o.order_id = op.order_id
JOIN products AS p
ON op.product_id = p.product_id
JOIN route_segments AS rs
ON o.order_id = rs.order_id
WHERE o.customer_id = 32 AND DATE(rs.segment_end_time) = '2024-02-13'
GROUP BY productID
ORDER BY totalWeight ASC;