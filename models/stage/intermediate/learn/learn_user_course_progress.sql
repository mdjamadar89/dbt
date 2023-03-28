{{ config(tags=["on_deploy"]) }}

WITH user_courses AS (
    SELECT
         item:uuid::VARCHAR AS user_uuid
        ,value:uuid::VARCHAR AS course_uuid
        ,value:path_uuid::VARCHAR AS path_uuid
        ,IFF(TRY_TO_TIMESTAMP(value:starting_date::VARCHAR) IS NULL, TRY_TO_TIMESTAMP(value:starting_date::VARCHAR, 'MM/DD/YYYY HH12:MI:SS AM'), TRY_TO_TIMESTAMP(value:starting_date::VARCHAR)) AS starting_date 
        ,IFF(TRY_TO_TIMESTAMP(value:completion_date::VARCHAR) IS NULL, TRY_TO_TIMESTAMP(value:completion_date::VARCHAR, 'MM/DD/YYYY HH12:MI:SS AM'), TRY_TO_TIMESTAMP(value:completion_date::VARCHAR)) AS completion_date 
        ,value:progress::DECIMAL(5,2) AS course_progress
        ,IFF(course_progress = 1, TRUE, FALSE) AS completed
        ,u.file_name
        ,COALESCE(load_timestamp, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
        ,COALESCE(op, 'L') AS op
    FROM repsites.learn_parquet.users AS u
        ,LATERAL FLATTEN(input => item:courses)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY value:uuid::VARCHAR, item:uuid::VARCHAR ORDER BY u.load_timestamp DESC, u.file_name DESC, u.file_row_number DESC) = 1
), distinct_courses AS (
    -- Including this to get the path UUID since it's not explicity included
    SELECT DISTINCT 
         path_uuid
        ,uuid AS course_uuid
    FROM  {{ ref('learn_courses') }} 
    WHERE learn_version = 2
)

SELECT
     uc.user_uuid
    ,dc.path_uuid
    ,uc.course_uuid
    ,uc.starting_date 
    ,uc.completion_date 
    ,uc.course_progress
    ,uc.completed
    ,uc.file_name
    ,uc.load_time
    ,2 AS learn_version
FROM user_courses AS uc
    LEFT JOIN distinct_courses AS dc
        ON dc.course_uuid = uc.course_uuid
WHERE op != 'D'

UNION ALL

SELECT
     REPLACE(SPLIT_PART(u.filename, '/', 4), '.json', '') AS user_uuid
    ,value:path_uuid::VARCHAR AS path_uuid
    ,value:uuid::VARCHAR AS course_uuid
    ,value:starting_date::TIMESTAMP_NTZ AS starting_date 
    ,value:completion_date::TIMESTAMP_NTZ AS completion_date 
    ,value:progress::DECIMAL(5,2) AS course_progress
    ,IFF(course_progress = 1, TRUE, FALSE) AS completed
    ,u.filename AS file_name
    ,COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
    ,1 AS learn_version

FROM repsites.learn.raw_user_state AS u
    ,LATERAL FLATTEN(input => item:courses)
WHERE REPLACE(SPLIT_PART(u.filename, '/', 4), '.json', '') IN (SELECT user_uuid FROM {{ ref('v2_users') }} ) = False -- exclude if in v2
QUALIFY ROW_NUMBER() OVER (PARTITION BY course_uuid, file_name ORDER BY COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) DESC) = 1