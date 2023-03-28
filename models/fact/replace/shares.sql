{{ config(tags=["every_6_hours", "on_deploy"]) }}

SELECT 
     MD5(t.user_id || t.schema_name) AS user_key
    ,MD5(COALESCE(t.market_id, 999999) || COALESCE(sites.site_id, 999999)) AS market_key
    ,MD5(COALESCE(sites.site_id, il.site_id)) AS site_key
    ,MD5(COALESCE(t.id , 999999) || COALESCE(sites.site_id, 999999)) AS unique_task_log_key
    ,il.created AS internal_link_created_date
    ,t.created AS task_log_created_date
    ,t.task_name
    ,t."TRIGGER"
    ,t.source
    ,il.controller
    ,il.slug
    ,il.domain
    ,il.target
    ,il.asset_id

FROM 
    repsites.mysql_complete.task_logs AS t
        LEFT JOIN
    repsites.mysql_complete.internal_links AS il
            ON t.internal_link_id = il.id
           AND t.schema_name = il.schema_name
        LEFT JOIN
     {{ ref('sites') }}
        ON LOWER(t.schema_name) = lower(sites.schema_name)
