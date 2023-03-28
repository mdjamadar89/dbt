{{ config(tags=["every_6_hours", "on_deploy"]) }}

SELECT MD5(internal_links.object:user_id::VARCHAR || internal_links.schema_name) AS user_key
      ,MD5(COALESCE(sites.site_id, users.site_id)) AS site_key
      ,MD5(internal_links.object:market_id::NUMBER(30,0) ) AS market_key
      ,MD5(COALESCE(internal_links.id , 999999) || COALESCE(internal_links.object:site_id::NUMBER(30,0), 999999)) AS unique_internal_link_key
      ,internal_links.object:created::TIMESTAMP_NTZ AS internal_link_created_date
      ,internal_link_clicks.object:created::TIMESTAMP_NTZ AS internal_link_click_date
      ,internal_links.object:target::VARCHAR AS internal_link_target
      ,NULLIF(internal_links.object:source::VARCHAR, '') AS internal_link_source
      ,internal_links.object:controller::VARCHAR AS internal_link_controller
      ,internal_links.object:slug::VARCHAR AS internal_link_slug
      ,internal_links.object:domain::VARCHAR AS internal_link_domain
      ,NULLIF(internal_links.object:asset_id::VARCHAR, '') AS internal_link_asset_id
      ,1 AS internal_link_clicks
      
FROM repsites.mysql_parquet_complete.internal_link_clicks
    LEFT JOIN repsites.mysql_parquet_complete.internal_links
        ON internal_links.id = internal_link_clicks.object:internal_link_id::NUMBER(30,0)
       AND internal_links.schema_name = internal_link_clicks.schema_name
    LEFT JOIN {{ ref('sites') }}
        ON LOWER(internal_link_clicks.schema_name) = LOWER(sites.schema_name)
    LEFT JOIN {{ ref('users') }}
        ON users.user_id = internal_links.object:user_id::VARCHAR
       AND users.schema_name = internal_links.schema_name
