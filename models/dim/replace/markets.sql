{{ config(tags=["every_6_hours"]) }}

SELECT
    MD5(COALESCE(m.id, 999999) || COALESCE(m.object:site_id::NUMBER(30,0), 999999)) AS market_key
    ,m.id AS market_id
    ,m.object:site_id::VARCHAR AS site_id
    ,m.object:name::VARCHAR AS name
    ,m.object:slug::VARCHAR AS slug
    ,m.object:locale::VARCHAR AS locale
    ,m.object:created::TIMESTAMP_NTZ AS created
    ,m.object:modified::TIMESTAMP_NTZ AS modified
    ,m.schema_name
FROM repsites.mysql_parquet_complete.markets m
    JOIN {{ ref('sites') }} AS s
        ON m.object:site_id::VARCHAR = s.site_id
       AND LOWER(m.schema_name) = LOWER(s.schema_name)