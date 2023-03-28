{{ config(tags=["on_deploy"]) }}

SELECT
    value:id::VARCHAR AS id
    ,value:parent_class::VARCHAR AS parent_class
    ,value:parent_uuid::VARCHAR AS parent_uuid
    ,SPLIT_PART(filename, '/', 4) AS site_id
    ,value:title::VARCHAR AS title
    ,value:earned::BOOLEAN AS earned
    ,value:image_url::VARCHAR AS image_url
    ,value:promo_code::VARCHAR AS promo_code
    ,value:published::BOOLEAN AS published
    ,value:sort_order::NUMBER(10,0) AS sort_order
    ,filename AS file_name
    ,COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
FROM repsites.learn.raw_awards
    ,LATERAL FLATTEN(input => item:awards)
QUALIFY ROW_NUMBER() OVER (PARTITION BY id, file_name ORDER BY COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) DESC) = 1