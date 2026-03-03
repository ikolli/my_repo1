select 
to_date(last_load_time) as load_date,
status,
table_catalog_name as db_name,
table_schema_name as schema_name,
table_name,
case when pipe_name is null then 'copy' else 'snowpipe' end as ingest_method,
sum(row_count) as row_count,
sum(row_parsed) as row_parsed,
avg(file_size) as avg_file_sixe_bytes,
sum(file_size) as total_file_size_bytes,
sum(file_size)/POWER(1024,1) AS total_file_size_kb,
sum(file_size)/power(1024,2) as total_file_size_mb,
sum(file_size)/power(1024,3) as total_file_size_gb,
sum(file_size)/power(1024,4) as total_file_size_tb
from "SNOWFLAKE"."ACCOUNT_USAGE"."COPY_HISTORY"
GROUP BY 1,2,3,4,5,6
order by 3,4,5,1,2;

select
to_date(start_time) as date,
warehouse_name,
sum(avg_running) as sum_running,
sum(avg_queued_load) as sum_queued
from "SNOWFLAKE"."ACCOUNT_USAGE"."WAREHOUSE_LOAD_HISTORY"
WHERE to_date(start_time) >= dateadd(month, -1, current_timestamp())
group by 1,2;

select
warehouse_name,
count(*) as query_count,
sum(bytes_scanned) as bytes_scanned,
sum(bytes_scanned*percentage_scanned_from_cache)as bytes_scannes_from_cache,
sum(bytes_scanned*percentage_scanned_from_cache)/sum(bytes_scanned) as percent_scanned_from_cache
from "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY"
WHERE start_time >= dateadd(month, -1, current_timestamp()) and bytes_scanned > 0 
group by 1 order by 5;

select user_name,
count(*) as query_count,
from "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY"
WHERE start_time >= dateadd(month, -1, current_timestamp()) 
and query_type not like 'CREATE%' 
AND PARTITIONS_SCANNED > (PARTITIONS_TOTAL * 0.95)
group by 1 order by 2 DESC;
