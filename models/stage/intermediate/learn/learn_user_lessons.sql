{{ config(tags=["on_deploy"]) }}

WITH user_lessons AS (
      SELECT
         item:uuid::VARCHAR AS user_uuid
        ,value:uuid::VARCHAR AS lesson_uuid
        ,value:course_uuid::VARCHAR AS course_uuid
        ,IFF(value:completion_date IS NULL, FALSE, TRUE) AS completed
        ,IFF(TRY_TO_TIMESTAMP(value:starting_date::VARCHAR) IS NULL, TRY_TO_TIMESTAMP(value:starting_date::VARCHAR, 'MM/DD/YYYY HH12:MI:SS AM'), TRY_TO_TIMESTAMP(value:starting_date::VARCHAR)) AS starting_date 
        ,IFF(TRY_TO_TIMESTAMP(value:completion_date::VARCHAR) IS NULL, TRY_TO_TIMESTAMP(value:completion_date::VARCHAR, 'MM/DD/YYYY HH12:MI:SS AM'), TRY_TO_TIMESTAMP(value:completion_date::VARCHAR)) AS completion_date 
        ,value:progress::DECIMAL(5,2) AS progress
        ,u.file_name
        ,COALESCE(load_timestamp::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
        ,COALESCE(op, 'L') AS op
    FROM repsites.learn_parquet.users AS u
        ,LATERAL FLATTEN(input => item:lessons)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY value:uuid::VARCHAR, item:uuid::VARCHAR ORDER BY u.load_timestamp DESC, u.file_name DESC, u.file_row_number DESC) = 1
), distinct_lessons AS (
    -- Including this to get the course UUID since it's not explicity included
    SELECT DISTINCT 
         course_uuid
        ,uuid AS lesson_uuid
    FROM {{ ref('learn_lessons_stage') }}  
    WHERE learn_version = 2
)

SELECT
     ul.user_uuid
    ,ul.lesson_uuid
    ,dl.course_uuid
    ,ul.completed
    ,ul.starting_date 
    ,ul.completion_date 
    ,ul.progress
    ,ul.file_name
    ,ul.load_time
    ,2 AS learn_version
FROM user_lessons AS ul
    LEFT JOIN distinct_lessons AS dl
        ON dl.lesson_uuid = ul.lesson_uuid
WHERE op != 'D'

UNION ALL

SELECT
    SPLIT_PART(filename, '/', 4) AS user_uuid
    ,value:uuid::VARCHAR AS lesson_uuid
    ,value:course_uuid::VARCHAR AS course_uuid
    ,value:completed::BOOLEAN AS completed
    ,value:starting_date::TIMESTAMP_NTZ AS starting_date 
    ,value:completion_date::TIMESTAMP_NTZ AS completion_date 
    ,value:progress::DECIMAL(5,2) AS progress
    ,filename AS file_name
    ,load_time::TIMESTAMP_NTZ AS load_time
    ,1 AS learn_version
FROM repsites.learn.raw_user_state
    ,LATERAL FLATTEN(input => item:lessons)
WHERE REPLACE(SPLIT_PART(filename, '/', 4), '.json', '') IN (SELECT user_uuid FROM {{ ref('v2_users') }} ) = False -- exclude if in v2
QUALIFY ROW_NUMBER() OVER (PARTITION BY lesson_uuid, file_name ORDER BY COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) DESC) = 1