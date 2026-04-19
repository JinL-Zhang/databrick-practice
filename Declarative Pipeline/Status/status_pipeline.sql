-- Create bronze status table
CREATE OR REFRESH STREAMING TABLE `data-pipeline`.bronze_db.status_bronze 
  COMMENT "Ingest raw JSON order status files from cloud storage"
  TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.reset.allowed" = false  -- prevent full table refreshes on the bronze table (even if click 'run pipeline with full table refresh')
  )
AS
SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  "${source}/status",
  format => "json"
);









-- Create the silver status table from bronze 
CREATE OR REFRESH STREAMING TABLE `data-pipeline`.silver_db.status_silver
(
  -- Drop rows if order_status_timestamp is not valid
  CONSTRAINT valid_timestamp
  EXPECT (order_status_timestamp > "2021-12-25")
  ON VIOLATION DROP ROW,

  -- Warn if order_status is not in the following options
  CONSTRAINT valid_order_status
  EXPECT (
    order_status IN (
      'on the way',
      'canceled',
      'return canceled',
      'delivered',
      'return processed',
      'placed',
      'preparing'
    )
  )
)
  COMMENT "Order with each status and timestamp"
  TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  order_id,
  order_status,
  timestamp(status_timestamp) as order_status_timestamp
FROM STREAM `data-pipeline`.bronze_db.status_bronze;










-- Create the Materialized View to Join Two Streaming Tables (orders + status)
-- This approach takes all rows from each streaming table and executes a full inner join operation
-- and incorporates optimizations where applicable.
CREATE OR REFRESH MATERIALIZED VIEW `data-pipeline`.gold_db.full_order_info_gold
  COMMENT "Joining the orders and order status silver tables to view all orders with each individual status per order"
  TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  orders.order_id,
  orders.order_timestamp,
  status.order_status,
  status.order_status_timestamp
  -- Notice that the STREAM keyword was not used when referencing the streaming tables to create the MV
FROM `data-pipeline`.silver_db.orders_silver orders
INNER JOIN `data-pipeline`.silver_db.status_silver status 
ON orders.order_id = status.order_id;












-- Create Gold Materialized Views for Cancelled and Delivered Orders using the joined data from above (orders and status)
CREATE OR REFRESH MATERIALIZED VIEW `data-pipeline`.gold_db.cancelled_orders_gold
  COMMENT "All cancelled orders"
  TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  order_id,
  order_timestamp,
  order_status,
  order_status_timestamp,
  datediff(day, order_timestamp, order_status_timestamp) as days_to_cancel
FROM `data-pipeline`.gold_db.full_order_info_gold
WHERE order_status = 'canceled';

-- DELIVERED ORDERS MV
CREATE OR REFRESH MATERIALIZED VIEW `data-pipeline`.gold_db.delivered_orders_gold
  COMMENT "All delivered orders"
  TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  order_id,
  order_timestamp,
  order_status,
  order_status_timestamp,
  datediff(DAY, order_timestamp, order_status_timestamp) AS days_to_delivery  -- calculate days to deliver
FROM `data-pipeline`.gold_db.full_order_info_gold
WHERE order_status = 'delivered';