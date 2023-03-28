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
), lessons AS (
    SELECT 
         c.course_uuid
        ,c.site_id
        ,c.path_uuid
        ,c.file_name
        ,c.load_time
        ,value:uuid::VARCHAR AS lesson_uuid
        ,value:quizzes AS quizzes
    FROM courses AS c
     ,LATERAL FLATTEN(input => lessons)
   WHERE value:quizzes IS NOT NULL

)

SELECT      
     value:uuid::VARCHAR AS uuid
    ,l.site_id
    ,value:title::VARCHAR AS title
    ,l.lesson_uuid AS lesson_uuid
    ,value:award_thresholds::VARIANT AS award_thresholds
    ,value:quiz_questions::VARIANT AS quiz_questions
    ,l.file_name
    ,l.load_time
    ,2 AS learn_version

FROM lessons AS l
     ,LATERAL FLATTEN(input => quizzes)

UNION ALL

SELECT 
     value:uuid::VARCHAR AS uuid
    ,SPLIT_PART(filename, '/', 4)::VARCHAR AS site_id
    ,value:title::VARCHAR AS title
    ,value:lesson_uuid::VARCHAR AS lesson_uuid
    ,value:award_thresholds::VARIANT AS award_thresholds
    ,value:quiz_questions::VARIANT AS quiz_questions
    ,filename AS file_name
    ,COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
    ,1 AS learn_version

FROM repsites.learn.raw_quizzes
    ,LATERAL FLATTEN(input => item:quizzes)
WHERE SPLIT_PART(filename, '/', 4)::VARCHAR IN (SELECT site_id FROM {{ ref('v2_sites') }}) = False
QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid, file_name ORDER BY COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) DESC) = 1
