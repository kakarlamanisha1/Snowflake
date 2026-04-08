CREATE OR REPLACE TABLE customer_summary AS
SELECT 
    c.customer_id,
    c.name,
    SUM(o.amount) AS total_spent,
    COUNT(o.order_id) AS total_orders
FROM customers c
JOIN orders o
ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name;
