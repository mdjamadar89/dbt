{{ config(tags=["every_6_hours", "on_deploy"]) }}

SELECT
     MD5(s.id) AS site_key
    ,s.id::VARCHAR AS site_id
    ,'REPSITES_' || UPPER(s.object:slug) AS schema_name
    ,s.object:title::VARCHAR AS title
    ,s.object:slug::VARCHAR AS slug
    ,s.object:removed::NUMBER(10,0) AS removed
    ,s.object:webroot::VARCHAR AS webroot
    ,s.object:site_domain::VARCHAR AS site_domain
    ,s.object:company_name::VARCHAR AS company_name
    ,s.object:copyright::VARCHAR AS copyright
    ,s.object:production_base_url::VARCHAR AS production_base_url
    ,s.object:google_analytics_id::VARCHAR AS google_analytics_id
    ,s.object:secondary_google_analytics_id::VARCHAR AS secondary_google_analytics_id
    ,s.object:created::TIMESTAMP_NTZ AS created
    ,s.object:modified::TIMESTAMP_NTZ AS modified
    ,CURRENT_DATE() AS current_date
    ,'Repsites' AS source

FROM repsites.mysql_parquet_complete.sites AS s
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY s.object:modified::TIMESTAMP_NTZ DESC ) = 1

UNION ALL

-- Add in VerbTeams:

SELECT
     MD5(c.object:"_id"::VARCHAR) AS site_key
    ,c.object:"_id"::VARCHAR AS site_id
    ,c.object:"name"::VARCHAR AS schema_name
    ,c.object:"name"::VARCHAR AS title
    ,c.object:"name"::VARCHAR AS slug
    ,0 AS removed
    ,NULL AS webroot
    ,NULL AS site_domain
    ,c.object:"name"::VARCHAR AS company_name
    ,NULL AS copyright
    ,NULL AS production_base_url
    ,NULL AS google_analytics_id
    ,NULL AS secondary_google_analytics_id
    ,NULL AS created
    ,c.load_timestamp::TIMESTAMP_NTZ AS modified
    ,CURRENT_DATE() AS current_date
    ,'verbTEAMS' AS source
FROM repsites.verb_teams_parquet.companies AS c