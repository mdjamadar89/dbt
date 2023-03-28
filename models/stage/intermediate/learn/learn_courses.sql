{{ config(tags=["on_deploy"]) }}

WITH paths AS (
    SELECT 
         value:uuid::VARCHAR AS path_uuid
        ,COALESCE(item:site_id_str::VARCHAR,item:site_id::VARCHAR) AS site_id
        ,value:courses AS courses
        ,file_name
        ,COALESCE(load_timestamp, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
        ,COALESCE(op, 'L') AS op
    FROM repsites.learn_parquet.path
        ,LATERAL FLATTEN(input => item:Paths)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY value:uuid::VARCHAR, COALESCE(item:site_id_str::VARCHAR,item:site_id::VARCHAR) ORDER BY load_timestamp DESC,file_name DESC, file_row_number DESC) = 1
)

SELECT 
     value:uuid::VARCHAR AS uuid
    ,p.site_id
    ,value:title::VARCHAR AS title
    ,p.path_uuid
    ,value:description::VARCHAR AS description
    ,value:lesson_number::NUMBER(10,0) AS lesson_number
    ,IFNULL(value:locked::BOOLEAN, FALSE) AS locked
    ,value:sort_order::NUMBER(10,0) AS sort_order
    ,value:display_size::VARCHAR AS display_size
    ,value:image_url::VARCHAR AS image_url
    ,p.file_name
    ,p.load_time
    ,2 AS learn_version

FROM paths AS p
 ,LATERAL FLATTEN(input => courses)
WHERE op != 'D'

UNION ALL

SELECT 
     value:uuid::VARCHAR AS uuid
    ,SPLIT_PART(filename, '/', 4)::VARCHAR AS site_id
    ,value:title::VARCHAR AS title
    ,value:path_uuid::VARCHAR AS path_uuid
    ,value:description::VARCHAR AS description
    ,value:lesson_number::NUMBER(10,0) AS lesson_number
    ,value:locked::BOOLEAN AS locked
    ,value:sort_order::NUMBER(10,0) AS sort_order
    ,value:display_size::VARCHAR AS display_size
    ,value:image_url::VARCHAR AS image_url
    ,filename AS file_name
    ,COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
    ,1 AS learn_version

FROM repsites.learn.raw_courses
    ,LATERAL FLATTEN(input => item:courses)
WHERE SPLIT_PART(filename, '/', 4)::VARCHAR IN (SELECT site_id FROM {{ ref('v2_sites') }}) = False
QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid, file_name ORDER BY COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) DESC) = 1