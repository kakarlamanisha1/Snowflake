CREATE OR REPLACE STREAM orders_stream
ON TABLE orders;

CREATE OR REPLACE TABLE order_updates (
    order_id INT,
    customer_id INT,
    amount NUMBER,
    action STRING
);

CREATE OR REPLACE TASK process_orders_task
WAREHOUSE = demo_wh
SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
INSERT INTO order_updates
SELECT 
    order_id,
    customer_id,
    amount,
    METADATA$ACTION
FROM orders_stream;

ALTER TASK process_orders_task RESUME;
