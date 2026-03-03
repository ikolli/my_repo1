/*--
Tasty Bytes is a fictitious, global food truck network, that is on a mission to serve unique food options with high quality items in a safe, convenient and cost effective way. In order to drive forward on their mission, Tasty Bytes is beginning to leverage the Snowflake AI Data Cloud.

Within this Worksheet, we will learn about processing Semi-Structured Data in Snowflake by diving into the VARIANT Data Type, Semi-Structured Data Processing combining Dot Notation and Lateral Flattening as well as View Creation.
--*/

/*----------------------------------------------------------------------------------
Step 0 - Setup

 We will start with loading all required data
----------------------------------------------------------------------------------*/

---> set the Role
USE ROLE SNOWFLAKE_LEARNING_ROLE;

---> set the Warehouse
USE WAREHOUSE SNOWFLAKE_LEARNING_WH;

---> set the Database
USE DATABASE SNOWFLAKE_LEARNING_DB;

---> set the Schema
SET schema_name = CONCAT(current_user(), '_WORKING_WITH_SEMI_STRUCTURED_DATA');
USE SCHEMA IDENTIFIER($schema_name);

---> create the Menu Table
CREATE OR REPLACE TABLE MENU
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

CREATE OR REPLACE STAGE blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);

---> copy the Menu file into the Menu table
COPY INTO menu
FROM @blob_stage/raw_pos/menu/;

---> confirm the empty Menu table exists
SELECT * FROM menu
LIMIT 10;


/*----------------------------------------------------------------------------------
Step 1 - Semi-Structured Data and the Variant Data Type

 As a Tasty Bytes Data Engineer, we have been tasked with profiling our Menu data and
 developing an Analytics layer View that exposes Dietary and Ingredient data to our
 downstream business users.
----------------------------------------------------------------------------------*/

-- let's take a look at a few columns in our Menu table we are receiving from our
-- Point of Sales (POS) system so we can see where our Dietary and Ingredient data is stored
SELECT TOP 10
    truck_brand_name,
    menu_type,
    menu_item_name,
    menu_item_health_metrics_obj
FROM menu;


-- based on the results above, the data we need to provide downstream is stored in the
-- Menu Item Health Metrics Object column. let's now use a SHOW COLUMNS command to
-- investigate what Data Type this column is.
SHOW COLUMNS IN menu;

    /**
     Variant: Snowflake can convert data from JSON, Avro, ORC, or Parquet format to an internal hierarchy of ARRAY,
      OBJECT, and VARIANT data and store that hierarchical data directly into a VARIANT column.
    **/


/*----------------------------------------------------------------------------------
Step 2 - Querying Semi-Structured Data

 The data stored within our Variant, Menu Item Health Metrics Object, column is JSON.

 Within this step, we will leverage Snowflake's Native Semi-Structured Support
 to query and flatten this column so that we can prepare to provide our downstream
 users with their requested data in an easy to understand, tabular format.
----------------------------------------------------------------------------------*/

-- to extract first-level elements from Variant columns, we can insert a Colon ":" between the Variant Column
-- name and first-level identifier. let's use this to extract Menu Item Id, and Menu Item Health Metrics Object
SELECT
    menu_item_health_metrics_obj:menu_item_id AS menu_item_id,
    menu_item_health_metrics_obj:menu_item_health_metrics AS menu_item_health_metrics
FROM menu;


/*--
 To convert Semi-Structured data to a relational representation we can use Flatten
 and to access elements from within a JSON object, we can use Dot or Bracket Notation.

 Let's now leverage both of these to extract our Ingredients into an Array Column.
--*/

--> Dot Notation and Lateral Flatten
SELECT
    m.menu_item_name,
    m.menu_item_health_metrics_obj:menu_item_id AS menu_item_id,
    obj.value:"ingredients"::ARRAY AS ingredients
FROM menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj
ORDER BY menu_item_id;


--> Bracket Notation and Lateral Flatten
SELECT
    m.menu_item_name,
    m.menu_item_health_metrics_obj['menu_item_id'] AS menu_item_id,
    obj.value['ingredients']::ARRAY AS ingredients
FROM menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj
ORDER BY menu_item_id;

    /**
     Array: A Snowflake ARRAY is similar to an array in many other programming languages.
      An ARRAY contains 0 or more pieces of data. Each element is accessed by specifying
      its position in the array.
    **/

/*--
 To complete our Semi-Structured processing, let's extract the remaining Dietary Columns
 using both Dot and Bracket Notation alongside the Ingredients Array.
--*/

--> Dot Notation and Lateral Flatten
SELECT
    m.menu_item_health_metrics_obj:menu_item_id AS menu_item_id,
    m.menu_item_name,
    obj.value:"ingredients"::VARIANT AS ingredients,
    obj.value:"is_healthy_flag"::VARCHAR(1) AS is_healthy_flag,
    obj.value:"is_gluten_free_flag"::VARCHAR(1) AS is_gluten_free_flag,
    obj.value:"is_dairy_free_flag"::VARCHAR(1) AS is_dairy_free_flag,
    obj.value:"is_nut_free_flag"::VARCHAR(1) AS is_nut_free_flag
FROM menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;


--> Bracket Notation and Lateral Flatten
SELECT
    m.menu_item_health_metrics_obj['menu_item_id'] AS menu_item_id,
    m.menu_item_name,
    obj.value['ingredients']::VARIANT AS ingredients,
    obj.value['is_healthy_flag']::VARCHAR(1) AS is_healthy_flag,
    obj.value['is_gluten_free_flag']::VARCHAR(1) AS is_gluten_free_flag,
    obj.value['is_dairy_free_flag']::VARCHAR(1) AS is_dairy_free_flag,
    obj.value['is_nut_free_flag']::VARCHAR(1) AS is_nut_free_flag
FROM menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;


/*----------------------------------------------------------------------------------
Step 3 - Providing Flattened Data to Business Users

 With all of the required data extracted, flattened and available in tabular form,
 we will now work to provide access to our Business Users.

 Within this step, we will promote a full output of the Menu table with the flattened
 columns to a new view. For simplicity, we will create a single view with menu data.
----------------------------------------------------------------------------------*/

-- to begin, let's add columns to our previous Dot Notation query and leverage it within a new Menu View
CREATE OR REPLACE VIEW menu_v
COMMENT = 'Menu level metrics including Truck Brands and Menu Item details including Cost, Price, Ingredients and Dietary Restrictions'
    AS
SELECT
    m.menu_id,
    m.menu_type_id,
    m.menu_type,
    m.truck_brand_name,
    m.menu_item_health_metrics_obj:menu_item_id::integer AS menu_item_id,
    m.menu_item_name,
    m.item_category,
    m.item_subcategory,
    m.cost_of_goods_usd,
    m.sale_price_usd,
    obj.value:"ingredients"::VARIANT AS ingredients,
    obj.value:"is_healthy_flag"::VARCHAR(1) AS is_healthy_flag,
    obj.value:"is_gluten_free_flag"::VARCHAR(1) AS is_gluten_free_flag,
    obj.value:"is_dairy_free_flag"::VARCHAR(1) AS is_dairy_free_flag,
    obj.value:"is_nut_free_flag"::VARCHAR(1) AS is_nut_free_flag
FROM menu m,
    LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj;

-- before moving on, let's use our view to take a look at the results for our Better Off Bread brand
SELECT
    truck_brand_name,
    menu_item_name,
    sale_price_usd,
    ingredients,
    is_healthy_flag,
    is_gluten_free_flag,
    is_dairy_free_flag,
    is_nut_free_flag
FROM menu_v
WHERE truck_brand_name = 'Better Off Bread';


/*----------------------------------------------------------------------------------
Step 4 - Leveraging Array Functions

 Within this step, we will address questions from
 the the Tasty Bytes Leadership Team related to our Food Truck Menus.

 Along the way we will see how Snowflake can provide a relational query experience
 over Semi-Structured data without having to make additional copies or conduct any
 complex data transformations.
----------------------------------------------------------------------------------*/

-- with recent Lettuce recalls in the news, which of our Menu Items include this as an Ingredient?
SELECT
    m.menu_item_id,
    m.menu_item_name,
    m.ingredients
FROM menu_v m
WHERE ARRAY_CONTAINS('Lettuce'::VARIANT, m.ingredients);

    /**
     Array_contains: The function returns TRUE if the specified value is present in the array.
    **/

-- which Menu Items across Menu Types contain overlapping Ingredients and what are those Ingredients?.
SELECT
    m1.truck_brand_name,
    m1.menu_item_name,
    m2.truck_brand_name AS overlap_brand,
    m2.menu_item_name AS overlap_menu_item_name,
    ARRAY_INTERSECTION(m1.ingredients, m2.ingredients) AS overlapping_ingredients
FROM menu_v m1
JOIN menu_v m2
    ON m1.menu_item_id <> m2.menu_item_id -- avoid joining the same menu item to itself
    AND m1.menu_type <> m2.menu_type
WHERE 1=1
    AND m1.item_category  <> 'Beverage' -- remove beverages
    AND ARRAYS_OVERLAP(m1.ingredients, m2.ingredients) -- return only those that overlap
ORDER BY ARRAY_SIZE(overlapping_ingredients) DESC; -- order by largest number of overlapping ingredients

    /**
     Array_intersection: Returns an array that contains the matching elements in the two input arrays.
     Arrays_overlap: Compares whether two arrays have at least one element in common
     Array_size: Returns the size of the input array
    **/

-- how many total Menu Items do we have and how many address Dietary Restrictions?
SELECT
    COUNT(DISTINCT menu_item_id) AS total_menu_items,
    SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count,
    SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count,
    SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count
FROM menu_v m;


-- how do the Plant Palace, Peking Truck and Better Off Bread Brands compare to each other?
    --> Snowsight Chart Type: Bar | Orientation: 1st Option | Grouping: 1st Option
        --> Y-Axis: BRAND_NAME | Bars: GLUTEN_FREE_ITEM_COUNT, DAIRY_FREE_ITEM_COUNT, NUT_FREE_ITEM_COUNT
SELECT
    m.truck_brand_name,
    SUM(CASE WHEN is_gluten_free_flag = 'Y' THEN 1 ELSE 0 END) AS gluten_free_item_count,
    SUM(CASE WHEN is_dairy_free_flag = 'Y' THEN 1 ELSE 0 END) AS dairy_free_item_count,
    SUM(CASE WHEN is_nut_free_flag = 'Y' THEN 1 ELSE 0 END) AS nut_free_item_count
FROM menu_v m
WHERE m.truck_brand_name IN ('Plant Palace', 'Peking Truck','Revenge of the Curds')
GROUP BY m.truck_brand_name;


/*----------------------------------------------------------------------------------
 Reset Scripts

  Run the scripts below to reset your account.
----------------------------------------------------------------------------------*/

-- drop the new Menu View
DROP VIEW IF EXISTS menu_v;

-- drop the Menu table
DROP TABLE IF EXISTS menu;

-- drop stage
DROP STAGE IF EXISTS blob_stage;

-- unset SQL Variable
UNSET schema_name;
