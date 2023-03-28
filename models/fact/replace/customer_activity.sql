{{ config(tags=["every_night"]) }}

SELECT 
     activity_timestamp
    ,user_key
    ,site_key
    ,market_key
    ,activity_name
    ,activity_type
    ,activity_details
    ,source_datasource
    ,source_name
FROM {{ ref('contact_activities') }}

UNION ALL 

SELECT 
     activity_timestamp
    ,user_key
    ,site_key
    ,market_key
    ,activity_name
    ,activity_type
    ,activity_details
    ,source_datasource
    ,source_name
FROM {{ ref('internal_link_clicks_billing') }}

UNION ALL 

SELECT 
     activity_timestamp
    ,user_key
    ,site_key
    ,market_key
    ,activity_name
    ,activity_type
    ,activity_details
    ,source_datasource
    ,source_name
FROM {{ ref('internal_links') }}

UNION ALL 

SELECT 
     activity_timestamp
    ,user_key
    ,site_key
    ,market_key
    ,activity_name
    ,activity_type
    ,activity_details
    ,source_datasource
    ,source_name
FROM {{ ref('myship_orders') }}

UNION ALL 

SELECT 
     activity_timestamp
    ,user_key
    ,site_key
    ,market_key
    ,activity_name
    ,activity_type
    ,activity_details
    ,source_datasource
    ,source_name
FROM {{ ref('task_logs') }}

UNION ALL 

SELECT 
     activity_timestamp
    ,user_key
    ,site_key
    ,market_key
    ,activity_name
    ,activity_type
    ,activity_details
    ,source_datasource
    ,source_name
FROM {{ ref('user_activities') }}