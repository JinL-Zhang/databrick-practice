-- A. Create the bronze streaming table from a JSON files in your volume
-- NOTE: read_files references the 'source' configuration key from your pipeline settings. 

CREATE OR REFRESH STREAMING TABLE `data-pipeline`.bronze_db.orders_bronze 
  COMMENT "Ingest order JSON files from cloud storage"       -- Adds a comment to the table
  TBLPROPERTIES (
      "quality" = "bronze",                -- Adds a simple table property to the table
      "pipelines.reset.allowed" = false    -- prevent full table refreshes on the bronze table (even you click "run pipeline with full table refresh, this table will not be refreshed" )
  )    
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(  -- Performs incremental ingestion with checkpoints using Auto Loader
    "${source}/orders",  -- Uses the source configuration variable set in the pipeline settings
    format => 'JSON'
);


-- B. Create the silver streaming table 
CREATE OR REFRESH STREAMING TABLE `data-pipeline`.silver_db.orders_silver
  ( -- Create the constraints for the silver table
   CONSTRAINT valid_notification EXPECT (notifications IN ('Y', 'x')), -- Warn
   CONSTRAINT valid_date EXPECT (order_timestamp > ('2021-12-26')) ON VIOLATION DROP ROW, -- Drop
   CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE -- Fail
  )
  COMMENT "Silver clean orders table"
  TBLPROPERTIES (
      "quality" = "silver"
  )
AS 
SELECT 
  order_id,
  timestamp(order_timestamp) AS order_timestamp, 
  customer_id,
  notifications
FROM STREAM `data-pipeline`.bronze_db.orders_bronze ; -- References the streaming orders_bronze table for incrementally processing


-- C. Create the materialized view aggregation from the orders_silver table with the summarization
CREATE OR REFRESH MATERIALIZED VIEW `data-pipeline`.gold_db.gold_orders_by_date 
  COMMENT "Aggregate gold data for downstream analysis"
  TBLPROPERTIES (
      "quality" = "gold"
  )
AS 
SELECT 
  date(order_timestamp) AS order_date, 
  count(*) AS total_daily_orders
FROM `data-pipeline`.silver_db.orders_silver  -- Aggregates the full orders_silver streaming table with optimizations where applicable
-- Do not add the STREAM keyword in FROM when creating a MV as it will identify that itself
GROUP BY date(order_timestamp);
 