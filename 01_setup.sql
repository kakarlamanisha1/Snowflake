-- Create DB & Schema
CREATE OR REPLACE DATABASE demo_db;
CREATE OR REPLACE SCHEMA demo_db.demo_schema;

USE DATABASE demo_db;
USE SCHEMA demo_schema;

-- Create Warehouse
CREATE OR REPLACE WAREHOUSE demo_wh
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

-- Create Tables
CREATE OR REPLACE TABLE customers (
    customer_id INT,
    name STRING,
    city STRING
);

CREATE OR REPLACE TABLE orders (
    order_id INT,
    customer_id INT,
    amount NUMBER,
    order_date DATE
);