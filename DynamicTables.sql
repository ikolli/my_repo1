-- =============================================================================
-- LOAD DATA
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE SNOWFLAKE_LEARNING_DB;
USE WAREHOUSE COMPUTE_WH;

-- Create user-specific schema based on current user
SET user_schema = CURRENT_USER() || '_DYNAMIC_TABLES';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER($user_schema);
USE SCHEMA IDENTIFIER($user_schema);

-- File format for CSV files
CREATE OR REPLACE FILE FORMAT csv_ff
  TYPE = 'CSV';

-- External stage pointing to Tasty Bytes public S3 bucket
CREATE OR REPLACE STAGE tasty_bytes_stage
  URL = 's3://sfquickstarts/tastybytes/'
  FILE_FORMAT = csv_ff;

-- Create raw menu table
CREATE OR REPLACE TABLE menu_raw
(
  menu_id NUMBER(19,0),
  menu_type_id NUMBER(38,0),
  menu_type VARCHAR,
  truck_brand_name VARCHAR,
  menu_item_id NUMBER(38,0),
  menu_item_name VARCHAR,
  item_category VARCHAR,
  item_subcategory VARCHAR,
  cost_of_goods_usd NUMBER(38,4),
  sale_price_usd NUMBER(38,4),
  menu_item_health_metrics_obj VARIANT
);

-- Load menu data from S3
COPY INTO menu_raw
FROM @tasty_bytes_stage/raw_pos/menu/;

-- This creates a dynamic table that calculates menu item profitability
-- and refreshes automatically to stay within 3 hours of the source data
CREATE OR REPLACE DYNAMIC TABLE menu_profitability
  TARGET_LAG = '3 hours'
  WAREHOUSE = COMPUTE_WH
  AS
SELECT
  -- Product identifiers
  menu_item_id,
  menu_item_name,
  truck_brand_name,
  menu_type,
  item_category,
  item_subcategory,

  -- Pricing information
  cost_of_goods_usd,
  sale_price_usd,

  -- Profitability calculations
  (sale_price_usd - cost_of_goods_usd) AS profit_usd,
  ROUND(
    ((sale_price_usd - cost_of_goods_usd) / NULLIF(sale_price_usd, 0)) * 100,
    2
  ) AS profit_margin_pct,

  -- Price categorization
  CASE
    WHEN sale_price_usd < 5 THEN 'Budget'
    WHEN sale_price_usd BETWEEN 5 AND 10 THEN 'Mid-Range'
    ELSE 'Premium'
  END AS price_tier

FROM menu_raw
WHERE sale_price_usd IS NOT NULL
  AND cost_of_goods_usd IS NOT NULL;

-- Query the dynamic table
SELECT
  truck_brand_name,
  menu_item_name,
  price_tier,
  profit_usd,
  profit_margin_pct
FROM menu_profitability
ORDER BY profit_margin_pct DESC
LIMIT 10;  

-- =============================================================================
-- INCREMENTAL REFRESH
-- =============================================================================

-- Create stored procedure to generate new menu items
CREATE OR REPLACE PROCEDURE generate_menu_items(num_rows INTEGER)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  items_before INTEGER;
  items_after INTEGER;
  items_inserted INTEGER;
BEGIN
  -- Capture count before insert
  SELECT COUNT(*) INTO :items_before FROM menu_raw;

  -- Insert new menu items based on existing data with randomized values
  INSERT INTO menu_raw (
    menu_id,
    menu_type_id,
    menu_type,
    truck_brand_name,
    menu_item_id,
    menu_item_name,
    item_category,
    item_subcategory,
    cost_of_goods_usd,
    sale_price_usd,
    menu_item_health_metrics_obj
  )
  SELECT
    (SELECT COALESCE(MAX(menu_id), 0) FROM menu_raw) + ROW_NUMBER() OVER (ORDER BY RANDOM()) AS menu_id,
    menu_type_id,
    menu_type,
    truck_brand_name,
    (SELECT COALESCE(MAX(menu_item_id), 0) FROM menu_raw) + ROW_NUMBER() OVER (ORDER BY RANDOM()) AS menu_item_id,
    'New Menu Item ' || ((SELECT COALESCE(MAX(menu_item_id), 0) FROM menu_raw) + ROW_NUMBER() OVER (ORDER BY RANDOM())) AS menu_item_name,
    item_category,
    item_subcategory,
    cost_of_goods_usd * (0.8 + UNIFORM(0, 0.4, RANDOM())) AS cost_of_goods_usd,
    sale_price_usd * (0.8 + UNIFORM(0, 0.4, RANDOM())) AS sale_price_usd,
    menu_item_health_metrics_obj
  FROM menu_raw
  WHERE menu_item_id IS NOT NULL
  ORDER BY RANDOM()
  LIMIT :num_rows;

  -- Capture count after insert
  SELECT COUNT(*) INTO :items_after FROM menu_raw;

  items_inserted := :items_after - :items_before;

  RETURN 'Successfully inserted ' || items_inserted::STRING || ' new menu items. Total items: ' || items_after::STRING;
END;
$$;

-- Example: Generate and insert 100 new menu items
CALL generate_menu_items(100);

-- Verify new rows were added
SELECT COUNT(*) AS total_rows FROM menu_raw;

-- =============================================================================
-- MANUAL REFRESH OF DYNAMIC TABLES
-- =============================================================================

/*
To observe incremental refresh now – rather than wait on the scheduled refresh – we manually refresh 
the dynamic tables to demonstrate incremental refresh behavior. 
Snowflake will automatically detect changes and perform an incremental refresh when possible.
*/

-- Refresh the menu_profitability dynamic table
ALTER DYNAMIC TABLE menu_profitability REFRESH;

-- =============================================================================
-- VERIFY INCREMENTAL REFRESH
-- =============================================================================

/*
Query the refresh history to verify that incremental refresh was used.
The refresh_action column will show 'INCREMENTAL' for incremental refreshes
and 'FULL' for full table refreshes.
*/

-- Check refresh history for menu_profitability
SELECT
  name,
  refresh_action,  -- Look for 'INCREMENTAL' vs 'FULL'
  state,
  refresh_start_time,
  refresh_end_time,
  DATEDIFF('second', refresh_start_time, refresh_end_time) AS duration_seconds
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE name = 'MENU_PROFITABILITY'
ORDER BY refresh_start_time DESC
LIMIT 5;

-- View refresh history for all dynamic tables
SELECT
  name,
  refresh_action,
  state,
  refresh_start_time,
  refresh_end_time,
  DATEDIFF('second', refresh_start_time, refresh_end_time) AS duration_seconds
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
ORDER BY refresh_start_time DESC
LIMIT 10;

-- =============================================================================
-- CONVERTING MATERIALIZED VIEW TO DYNAMIC TABLE
-- =============================================================================

-- Create a materialized view showing menu summary by brand and category
-- Optionally, recreate it as a dynamic table for better control and performance
CREATE OR REPLACE dynamic table menu_summary_dt
    TARGET_LAG = '1 hour'  -- Control refresh timing
     WAREHOUSE = COMPUTE_WH
AS
SELECT
  truck_brand_name,
  menu_type,
  item_category,

  -- Aggregated metrics
  COUNT(*) AS item_count,
  ROUND(AVG(cost_of_goods_usd), 2) AS avg_cost_usd,
  ROUND(AVG(sale_price_usd), 2) AS avg_price_usd,
  ROUND(AVG(sale_price_usd - cost_of_goods_usd), 2) AS avg_profit_usd,
  ROUND(
    AVG(((sale_price_usd - cost_of_goods_usd) / NULLIF(sale_price_usd, 0)) * 100),
    2
  ) AS avg_margin_pct,
  MIN(sale_price_usd - cost_of_goods_usd) AS min_profit_usd,
  MAX(sale_price_usd - cost_of_goods_usd) AS max_profit_usd

FROM menu_raw
WHERE sale_price_usd IS NOT NULL
  AND cost_of_goods_usd IS NOT NULL
GROUP BY
  truck_brand_name,
  menu_type,
  item_category;

-- Query the materialized view
SELECT
  truck_brand_name,
  menu_type,
  item_category,
  item_count,
  avg_profit_usd,
  avg_margin_pct
FROM menu_summary_mv
ORDER BY avg_margin_pct DESC
LIMIT 10;

-- Query the dynamic table
SELECT
  truck_brand_name,
  menu_type,
  item_category,
  item_count,
  avg_profit_usd,
  avg_margin_pct
FROM menu_summary_dt
ORDER BY avg_margin_pct DESC
LIMIT 10;

-- OPTIONAL: Drop the materialized view (if converting)
-- Uncomment to drop:
DROP MATERIALIZED VIEW IF EXISTS menu_summary_mv;

-- =============================================================================
-- CHANGE DATA CAPTURE (CDC) COMPARISON
-- =============================================================================

-- Create stream on source table
CREATE OR REPLACE STREAM menu_changes_stream
  ON TABLE menu_raw;

-- Target table with transformations applied
CREATE OR REPLACE TABLE menu_profitability_cdc
(
  menu_item_id NUMBER(38,0),
  menu_item_name VARCHAR,
  truck_brand_name VARCHAR,
  profit_usd NUMBER(38,4),
  profit_margin_pct NUMBER(38,2),
  updated_at TIMESTAMP_NTZ
);

-- Creatae a Task to process the stream and update the target table
CREATE OR REPLACE TASK update_menu_profitability
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '3 HOURS'
WHEN
  SYSTEM$STREAM_HAS_DATA('menu_changes_stream')
AS
  MERGE INTO menu_profitability_cdc t
  USING (
    SELECT 
      menu_item_id,
      menu_item_name,
      truck_brand_name,
      sale_price_usd - cost_of_goods_usd AS profit_usd,
      ROUND(((sale_price_usd - cost_of_goods_usd) / NULLIF(sale_price_usd, 0)) * 100, 2) AS profit_margin_pct,
      METADATA$ACTION
    FROM menu_changes_stream
  ) s
  ON t.menu_item_id = s.menu_item_id
  WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' THEN 
    DELETE
  WHEN MATCHED THEN 
    UPDATE SET
      t.menu_item_name = s.menu_item_name,
      t.truck_brand_name = s.truck_brand_name,
      t.profit_usd = s.profit_usd,
      t.profit_margin_pct = s.profit_margin_pct,
      t.updated_at = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN 
    INSERT (menu_item_id, menu_item_name, truck_brand_name, profit_usd, profit_margin_pct, updated_at)
    VALUES (s.menu_item_id, s.menu_item_name, s.truck_brand_name, s.profit_usd, s.profit_margin_pct, CURRENT_TIMESTAMP());

-- Insert some dummy data to load the stream with changes
INSERT INTO menu_profitability_cdc
SELECT 
  menu_item_id, menu_item_name, truck_brand_name,
  sale_price_usd - cost_of_goods_usd AS profit_usd,
  ROUND(((sale_price_usd - cost_of_goods_usd) / NULLIF(sale_price_usd, 0)) * 100, 2) AS profit_margin_pct,
  CURRENT_TIMESTAMP() AS updated_at
FROM menu_raw
WHERE sale_price_usd IS NOT NULL AND cost_of_goods_usd IS NOT NULL;

-- Manually run the task to test
EXECUTE TASK update_menu_profitability;

-- Query the traditional CDC results
SELECT
  menu_item_id,
  menu_item_name,
  truck_brand_name,
  profit_usd,
  profit_margin_pct,
  updated_at
FROM menu_profitability_cdcdepends on 
ORDER BY profit_margin_pct DESC
LIMIT 10;

-- =============================================================================
-- APPROACH B: CDC WITH DYNAMIC TABLES
-- =============================================================================

-- Create dynamic table with same CDC logic
-- Same result, declarative approach - already created earlier in demo
CREATE OR REPLACE DYNAMIC TABLE menu_profitability_dt
  TARGET_LAG = '3 HOURS'
  WAREHOUSE = COMPUTE_WH
AS
SELECT
  menu_item_id,
  menu_item_name,
  truck_brand_name,
  (sale_price_usd - cost_of_goods_usd) AS profit_usd,
  ROUND(((sale_price_usd - cost_of_goods_usd) / NULLIF(sale_price_usd, 0)) * 100, 2) AS profit_margin_pct
FROM menu_raw;

-- Query the dynamic table results
SELECT
  menu_item_id,
  menu_item_name,
  truck_brand_name,
  profit_usd,
  profit_margin_pct
FROM menu_profitability_dt
ORDER BY profit_margin_pct DESC
LIMIT 10;

-- Suspend the task to stop running: be sure to run this!
ALTER TASK update_menu_profitability SUSPEND;

-- =============================================================================
-- CLEANUP (Optional)
-- =============================================================================

DROP TASK IF EXISTS update_menu_profitability;
DROP STREAM IF EXISTS menu_changes_stream;
DROP TABLE IF EXISTS menu_profitability_cdc;

DROP DYNAMIC TABLE IF EXISTS menu_profitability_dt;

DROP PROCEDURE IF EXISTS generate_menu_items(INTEGER);
DROP DYNAMIC TABLE IF EXISTS menu_profitability;
DROP MATERIALIZED VIEW IF EXISTS menu_summary_mv;
DROP TABLE IF EXISTS menu_raw;
DROP STAGE IF EXISTS tasty_bytes_stage;
DROP FILE FORMAT IF EXISTS csv_ff;

-- Drop the user-specific schema (optional - uncomment if you want to remove the schema entirely)
-- DROP SCHEMA IF EXISTS IDENTIFIER($user_schema);