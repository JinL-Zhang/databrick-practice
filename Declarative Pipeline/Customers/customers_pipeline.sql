-- Create the streaming customer bronze view from the customer data source
CREATE OR REFRESH STREAMING TABLE `data-pipeline`.bronze_db.customers_bronze
  COMMENT "RAw data from customers CDC feed"
  TBLPROPERTIES(
    "quality" = "bronze",
    "pipelines.reset.allowed" = false -- prevent full table refreshes on the bronze table
  )
AS 
SELECT *,
  current_timestamp() processing_time,
  _metadata.file_name as source_file
FROM STREAM read_files(
  "${source}/customers",
  format => "json"
);





-- Create the bronze cleaing streaming data table with data qulity enforcement
CREATE OR REFRESH STREAMING TABLE `data-pipeline`.bronze_db.customers_bronze_clean
(
  CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE, -- fail the pipeline if customer id (unique identifier ) is not there
  CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW, -- drop the rows that have no valid operation
  CONSTRAINT valid_name EXPECT (name IS NOT NULL OR operation = 'DELETE'),
  CONSTRAINT valid_address EXPECT (
    (address IS NOT NULL AND
    city IS NOT NULL AND
    state IS NOT NULL AND
    zip_code IS NOT NULL) OR
    operation = 'DELETE'
  ),
  CONSTRAINT valid_email EXPECT (
      rlike(email, '^([a-zA-Z-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') OR
      operation = 'DELETE' 
  ) ON VIOLATION DROP ROW -- drop the rows with invalid email format
)
COMMENT "CLean raw bronze timestamp column and add data quality constraints"
AS
SELECT *, CAST(from_unixtime(timestamp) AS timestamp) as timestamp_datetime
FROM STREAM `data-pipeline`.bronze_db.customers_bronze;





-- Processing CDC data with AUTO CDC INTO
-- Create the empty streaming target silver table first
CREATE OR REFRESH STREAMING TABLE `data-pipeline`.silver_db.customers_silver
COMMENT "SCD Type 1 Historical Customers Data";

-- Create the CDC flow
CREATE FLOW scd_type_1_flow AS
AUTO CDC INTO `data-pipeline`.silver_db.customers_silver -- define the target table for the CDC flow to apply the changes to
FROM STREAM `data-pipeline`.bronze_db.customers_bronze_clean -- define the source table for the CDC flow to read the operations (updates, deletes, inserts)
KEYS (customer_id) -- define the primary joining key for identifying records to be applied operations to (if not matched, insert the new row to the target table)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY timestamp_datetime -- defines the order of operations for applying changes
COLUMNS * EXCEPT (operation, timestamp, _rescued_data) -- select columns 
STORED AS SCD TYPE 1

-- Note that the auto cdc will show 'Upserted' records to show the # of records modified (not deleted)