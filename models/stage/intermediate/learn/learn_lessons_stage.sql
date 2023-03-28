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
), courses AS (
    SELECT 
         value:uuid::VARCHAR AS course_uuid
        ,p.site_id
        ,p.path_uuid
        ,p.file_name
        ,p.load_time
        ,value:lessons AS lessons
    FROM paths AS p
     ,LATERAL FLATTEN(input => courses)
    WHERE op != 'D'
)

SELECT 
     value:uuid::VARCHAR AS uuid
    ,c.site_id::VARCHAR AS site_id
    ,value:title::VARCHAR AS title
    ,c.course_uuid
    ,value:completion_time::VARCHAR AS completion_time
    ,value:description::VARCHAR AS description
    ,value:sections::VARIANT AS sections
    ,value:sort_order::NUMBER(10,0) AS sort_order
    ,c.file_name
    ,c.load_time
    ,2 AS learn_version
FROM courses AS c
     ,LATERAL FLATTEN(input => lessons)

UNION ALL

SELECT 
     value:uuid::VARCHAR AS uuid
    ,SPLIT_PART(filename, '/', 4) AS site_id
    ,value:title::VARCHAR AS title
    ,value:course_uuid::VARCHAR AS course_uuid
    ,value:completion_time::VARCHAR AS completion_time
    ,value:description::VARCHAR AS description
    ,value:sections::VARIANT AS sections
    ,value:sort_order::NUMBER(10,0) AS sort_order
    ,filename AS file_name
    ,COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
    ,1 AS learn_version

FROM repsites.learn.raw_lessons
    ,LATERAL FLATTEN(input => item:lessons)
WHERE SPLIT_PART(filename, '/', 4)::VARCHAR IN (SELECT site_id FROM {{ ref('v2_sites') }}) = False
QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid, file_name ORDER BY COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) DESC) = 1