/*--
Tasty Bytes is a fictitious, global food truck network, that is on a mission to serve unique food options with high quality items in a safe, convenient and cost effective way. In order to drive forward on their mission, Tasty Bytes is beginning to leverage the Snowflake AI Data Cloud.

Within this Worksheet, we will learn about Virtual Warehouses in Snowflake by diving into Snowflake Warehouses and their configurability, Resource Monitors, Account and Warehouse Level Timeout Parameters, Budgets and Exploring Cost.
--*/

/*----------------------------------------------------------------------------------
Step 0 - Setup

 We will start with building all required Snowflake objects and setting the context
----------------------------------------------------------------------------------*/

---> set the Role
USE ROLE SYSADMIN;

---> set the Warehouse
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

---> set the Database
USE DATABASE SNOWFLAKE_LEARNING_DB;

---> set the Schema
SET schema_name = CONCAT(current_user(), '_GET_STARTED_WITH_COMPUTE');
USE SCHEMA IDENTIFIER($schema_name);


---> create the order_header table
CREATE OR REPLACE TABLE order_header
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
);

---> create the Raw Menu Table
CREATE OR REPLACE TABLE menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

---> create the customer_loyalty table
CREATE OR REPLACE TABLE customer_loyalty
(
    customer_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    postal_code VARCHAR(16777216),
    preferred_language VARCHAR(16777216),
    gender VARCHAR(16777216),
    favourite_brand VARCHAR(16777216),
    marital_status VARCHAR(16777216),
    children_count VARCHAR(16777216),
    sign_up_date DATE,
    birthday_date DATE,
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
);

CREATE OR REPLACE STAGE blob_stage_csv
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);

-- We will come back to loading the data!

/*----------------------------------------------------------------------------------
Step 1 - Virtual Warehouses and Settings

 As a Tasty Bytes Snowflake Administrator we have been tasked with gaining an
 understanding of the features Snowflake provides to help ensure proper compute
 resources and guardrails are in place before we begin extracting value from our data.

 Within this step, we will create our first Snowflake Warehouse, which can be
 thought of as virtual compute.

 Snowflake recommends starting with the smallest sized Warehouse possible for the
 assigned workload, so for our Test Warehouse we will create it as an Extra Small.
----------------------------------------------------------------------------------*/


-- let's create our own Test Warehouse and reference the section below to understand each parameter is handling
CREATE OR REPLACE WAREHOUSE tastybytes_test_wh WITH
COMMENT = 'test warehouse for tasty bytes'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xsmall'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = true -- turn on
    INITIALLY_SUSPENDED = true;

    /**
     1) Warehouse Type: Warehouses are required for queries, as well as all DML operations, including
         loading data into tables. Snowflake supports Standard (most-common) or Snowpark-optimized
          Warehouse Types. Snowpark-optimized warehouses should be considered for memory-intensive
          workloads.

     2) Warehouse Size: Size specifies the amount of compute resources available per cluster in a warehouse.
         Snowflake supports X-Small through 6X-Large sizes.

     3) Max Cluster Count: With multi-cluster warehouses, Snowflake supports allocating, either statically
         or dynamically, additional clusters to make a larger pool of compute resources available.
         A multi-cluster warehouse is defined by specifying the following properties:
            - Min Cluster Count: Minimum number of clusters, equal to or less than the maximum (up to 10).
            - Max Cluster Count: Maximum number of clusters, greater than 1 (up to 10).

     4) Scaling Policy: Specifies the policy for automatically starting and shutting down clusters in a
         multi-cluster warehouse running in Auto-scale mode.

     5) Auto Suspend: By default, Auto-Suspend is enabled. Snowflake automatically suspends the warehouse
         if it is inactive for the specified period of time, in our case 60 seconds.

     6) Auto Resume: By default, auto-resume is enabled. Snowflake automatically resumes the warehouse
         when any statement that requires a warehouse is submitted and the warehouse is the
         current warehouse for the session.

     7) Initially Suspended: Specifies whether the warehouse is created initially in the ‘Suspended’ state.
    **/


/*----------------------------------------------------------------------------------
Step 2 - Resuming, Suspending and Scaling a Warehouse

 With a Warehouse created, let's now use it to answer a few questions from the
 business. While doing so we will learn how to resume, suspend and elastically
 scale the Warehouse.
----------------------------------------------------------------------------------*/

-- let's first set our Test Warehouse context
USE WAREHOUSE tastybytes_test_wh;


-- to showcase Snowflakes elastic scalability let's scale our Warehouse up and run a few larger, aggregation queries
ALTER WAREHOUSE tastybytes_test_wh SET warehouse_size = 'XLarge';

-- Let's start by loading some larger tables (particularly order_header) into Snowflake
---> copy the Menu file into the Menu table
COPY INTO menu
FROM @blob_stage_csv/raw_pos/menu/;

---> copy the Menu file into the Order_header table,
    -- NOTE: this table is >200,000,000 rows large takes about 2 minutes
COPY INTO order_header
FROM @blob_stage_csv/raw_pos/order_header/;

---> copy the Menu file into the customer_loyalty table
COPY INTO customer_loyalty
FROM @blob_stage_csv/raw_customer/customer_loyalty/;

-- Now we can run a large join to gain insight into customer orders
-- what are the total orders and total sales volumes for our top customer loyalty members?
SELECT
    o.customer_id,
    CONCAT(cl.first_name, ' ', cl.last_name) AS name,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(o.order_total) AS total_sales
FROM order_header o
JOIN customer_loyalty cl
    ON o.customer_id = cl.customer_id
GROUP BY o.customer_id, name
ORDER BY order_count DESC;

-- let's now scale our Test Warehouse back down
ALTER WAREHOUSE tastybytes_test_wh SET warehouse_size = 'XSmall';

-- what menu items do we serve at our Plant Palace branded trucks?
    --> NOTE: this is a smaller query that does not require as much compute
SELECT
    m.menu_type,
    m.truck_brand_name,
    m.menu_item_id,
    m.menu_item_name
FROM menu m
WHERE truck_brand_name = 'Plant Palace';

-- and now manually Suspend it
    --> NOTE: if you receive "Invalid state. Warehouse cannot be suspended." the auto_suspend we configured earlier has already occured
ALTER WAREHOUSE tastybytes_test_wh SUSPEND;


/*----------------------------------------------------------------------------------
Step 3 - Managing Warehouses with Session Timeout Parameters

 Within this step, let's now make sure we are protecting ourselves from bad,
 long running queries.

 To do this we will adjust two Statement Timeout Parameters on our Test Warehouse.
----------------------------------------------------------------------------------*/

-- to begin, let's look at the Statement Parameters for our Test Warehouse
SHOW PARAMETERS LIKE 'STATEMENT%' IN WAREHOUSE tastybytes_test_wh;


-- let's start by adjusting the 2 Statement Parameters related to Query Timeouts
--> 1) adjust Statement Timeout on the Test Warehouse to 30 minutes
ALTER WAREHOUSE tastybytes_test_wh
    SET statement_timeout_in_seconds = 1800; -- 1800 seconds = 30 minutes


--> 2) adjust Statement Queued Timeout on the Test Warehouse to 10 minutes
ALTER WAREHOUSE tastybytes_test_wh
    SET statement_queued_timeout_in_seconds = 600; -- 600 seconds = 10 minutes

    /**
     Statement Timeout in Seconds: Timeout in seconds for statements: statements are automatically canceled if they
      run for longer; if set to zero, max value (604800) is  enforced.

     Statement Queued in Second: Timeout in seconds for queued statements: statements will automatically be
      canceled if they are queued on a warehouse for longer than this  amount of time; disabled if set to zero.
    **/

/*----------------------------------------------------------------------------------
Step 4 - Resource Monitors

 With a Test Warehouse in place, let's now leverage Snowflake's Resource Monitors
 to ensure the Warehouse has a monthly quota. This will also allow Admins to monitor
 credit consumption and trigger Warehouse suspension if the quota is surpassed.

 Within this step we will create our Resource Monitor using SQL but these can also
 be deployed and monitored in Snowsight by navigating to Admin -> Cost Management.
----------------------------------------------------------------------------------*/

   /**
     Resource Monitor: A resource monitor can be used to monitor credit usage by virtual warehouses
      and the cloud services needed to support those warehouses. If desired, the warehouse can be
      suspended when it reaches a credit limit.
    **/

-- to begin we will assume the role of Accountadmin
USE ROLE accountadmin;

-- create our Resource Monitor
CREATE OR REPLACE RESOURCE MONITOR tastybytes_test_rm
WITH
    CREDIT_QUOTA = 50 -- set the quota to 100 credits
    FREQUENCY = monthly -- reset the monitor monthly
    START_TIMESTAMP = immediately -- begin tracking immediately
    TRIGGERS
        ON 75 PERCENT DO NOTIFY -- notify accountadmins at 75%
        ON 100 PERCENT DO SUSPEND -- suspend warehouse at 100 percent, let queries finish
        ON 110 PERCENT DO SUSPEND_IMMEDIATE; -- suspend warehouse and cancel all queries at 110 percent


-- with the Resource Monitor created, apply it to our Test Warehouse
ALTER WAREHOUSE tastybytes_test_wh
    SET RESOURCE_MONITOR = tastybytes_test_rm;

/*----------------------------------------------------------------------------------
Step 5 - Tag Objects to Attribute Spend

 Within this step, we will help our Finance department attribute consumption costs
 for the Test Warehouse to our Development Team.

 We will create a Tag object for associating Cost Centers to Database
 Objects and Warehouses and leverage it to assign the Development Team Cost Center
 to our Test Warehouse.
----------------------------------------------------------------------------------*/

    /**
     Tag: A tag is a schema-level object that can be assigned to another Snowflake object.
      A tag can be assigned an arbitrary string value upon assigning the tag to a Snowflake object.
      Snowflake stores the tag and its string value as a key-value pair.
    **/

-- first, we will create our Cost Center Tag
CREATE OR REPLACE TAG cost_center;


-- now we use the Tag to attach the Development Team Cost Center to the Test Warehouse
ALTER WAREHOUSE tastybytes_test_wh SET TAG cost_center = 'DEVELOPMENT_TEAM';


/*----------------------------------------------------------------------------------
Step 6 - Exploring Cost with Snowsight

Snowflake also provides many ways to visually inspect Cost data within Snowsight.
In this step, we will walk through the click path to access a few of these pages.

To access an overview of incurred costs within Snowsight:
    1. Select Admin » Cost Management.
    2. Select a warehouse to use to view the usage data.
        • Snowflake recommends using an X-Small warehouse for this purpose.
    3. Select Account Overview.

To access and drill down into overall cost within Snowsight:
    1. Select Admin » Cost Management.
    2. Select a warehouse to use to view the usage data.
        • Snowflake recommends using an X-Small warehouse for this purpose.
    3. Select Consumption.
    4. Select All Usage Types from the drop-down list.
----------------------------------------------------------------------------------*/


/*----------------------------------------------------------------------------------
 Reset Scripts

  Run the scripts below to reset your account to the state required to re-run
  this vignette.
----------------------------------------------------------------------------------*/
USE ROLE accountadmin;

-- drop Test Warehouse
DROP WAREHOUSE IF EXISTS tastybytes_test_wh;

-- drop Cost Center Tag
DROP TAG IF EXISTS cost_center;

-- drop Resource Monitor
DROP RESOURCE MONITOR IF EXISTS tastybytes_test_rm;

-- unset SQL Variable
UNSET schema_name;
