{{ config(tags=["on_deploy"]) }}



WITH current_state AS (
SELECT 
     value:sort_order::NUMBER(10, 0) AS course_number
    ,value:uuid::VARCHAR AS uuid
    ,COALESCE(item:site_id_str::VARCHAR,item:site_id::VARCHAR) AS site_id
    ,value:description::VARCHAR AS description
    ,value:image_url::VARCHAR AS image_url
    ,value:locale::VARCHAR AS locale
    ,value:markets::VARIANT AS markets
    ,value:sort_order::NUMBER(10, 0) AS sort_order
    ,value:title::VARCHAR AS title
    ,file_name AS file_name
    ,COALESCE(load_timestamp::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
    ,2 AS learn_version
    ,COALESCE(op, 'L') AS op
FROM repsites.learn_parquet.path
    ,LATERAL FLATTEN(input => item:Paths)
QUALIFY ROW_NUMBER() OVER (PARTITION BY value:uuid::VARCHAR, item:site_id::NUMBER(10, 0) ORDER BY load_timestamp DESC,file_name DESC, file_row_number DESC) = 1
)

SELECT 
     course_number
    ,uuid
    ,site_id
    ,description
    ,image_url
    ,locale
    ,markets
    ,sort_order
    ,title
    ,file_name
    ,load_time
    ,learn_version 
FROM current_state
WHERE op != 'D'

UNION ALL

SELECT 
     value:course_number::NUMBER(10,0) AS course_number
    ,value:uuid::VARCHAR AS uuid
    ,SPLIT_PART(filename, '/', 4)::VARCHAR AS site_id
    ,value:description::VARCHAR AS description
    ,value:image_url::VARCHAR AS image_url
    ,value:locale::VARCHAR AS locale
    ,value:markets::VARIANT AS markets
    ,value:sort_order::NUMBER(10, 0) AS sort_order
    ,value:title::VARCHAR AS title
    ,filename AS file_name
    ,COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
    ,1 AS learn_version
    
FROM repsites.learn.raw_paths
    ,LATERAL FLATTEN(input => item:paths)
WHERE SPLIT_PART(filename, '/', 4)::VARCHAR IN (SELECT site_id FROM {{ ref('v2_sites') }} ) = False
QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid, file_name ORDER BY COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) DESC) = 1
