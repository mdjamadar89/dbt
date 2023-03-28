{{ config(tags=["on_deploy"]) }}

WITH schema_list AS (
    SELECT DISTINCT 
         'MYSQL' AS snowflake_schema
        ,UPPER(table_schema) AS table_schema
    FROM repsites.mysql.information_schema_tables_query_results
    
    UNION ALL
    
    SELECT DISTINCT 
         'MYSQL_STANDALONE' AS snowflake_schema
        ,UPPER(database_names) AS table_schema
    FROM data_warehouse.stage.standalone_database_names
)

SELECT 
     s.snowflake_schema AS table_schema
    ,s.table_schema || '_' || UPPER(t.table_names) AS table_name
FROM schema_list AS s
    CROSS JOIN data_warehouse.stage.table_names AS t