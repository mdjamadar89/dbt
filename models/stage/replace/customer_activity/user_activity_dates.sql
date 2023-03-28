{{ config(tags=["every_6_hours", "on_deploy"]) }}

SELECT 
     MD5(ua.object:user_id::NUMBER(20,0) || ua.schema_name) AS user_key
    ,MIN(ua.object:created::TIMESTAMP_NTZ) AS first_user_activity_date
    ,MAX(ua.object:created::TIMESTAMP_NTZ) AS last_user_activity_date
FROM repsites.mysql_parquet_complete.user_activities AS ua
GROUP BY 1