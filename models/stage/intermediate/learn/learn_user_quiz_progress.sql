{{ config(tags=["on_deploy"]) }}

WITH user_quizzes AS (
      SELECT
         item:uuid::VARCHAR AS user_uuid
        ,value:uuid::VARCHAR AS quiz_uuid
        ,value:lesson_uuid::VARCHAR AS lesson_uuid
        ,value:passed::BOOLEAN AS passed
        ,value:progress::DECIMAL(5,2) AS progress
        ,u.file_name
        ,COALESCE(load_timestamp, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
        ,COALESCE(op, 'L') AS op
    FROM repsites.learn_parquet.users AS u
        ,LATERAL FLATTEN(input => item:quizzes)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY value:uuid::VARCHAR, item:uuid::VARCHAR ORDER BY u.load_timestamp DESC, u.file_name DESC, u.file_row_number DESC) = 1
), distinct_quizzes AS (
    -- Including this to get the lesson UUID since it's not explicity included
    SELECT DISTINCT 
         lesson_uuid
        ,uuid AS quiz_uuid
    FROM {{ ref('learn_quizzes') }}  
    WHERE learn_version = 2
)

SELECT
     uq.user_uuid
    ,COALESCE(uq.quiz_uuid, dq.quiz_uuid) AS quiz_uuid
    ,dq.lesson_uuid
    ,uq.passed
    ,uq.progress
    ,uq.file_name
    ,uq.load_time
    ,2 AS learn_version
FROM user_quizzes AS uq
    LEFT JOIN distinct_quizzes AS dq
        ON dq.quiz_uuid = uq.quiz_uuid
WHERE op != 'D'

UNION ALL

SELECT
     REPLACE(SPLIT_PART(u.filename, '/', 4), '.json', '') AS user_uuid
    ,value:uuid::VARCHAR AS quiz_uuid
    ,value:lesson_uuid::VARCHAR AS lesson_uuid
    ,value:passed::BOOLEAN AS passed
    ,value:progress::DECIMAL(5,2) AS progress
    ,u.filename AS file_name
    ,COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
    ,1 AS learn_version

FROM repsites.learn.raw_user_state AS u
    ,LATERAL FLATTEN(input => item:quizzes)
WHERE REPLACE(SPLIT_PART(u.filename, '/', 4), '.json', '') IN (SELECT user_uuid FROM {{ ref('v2_users') }} ) = False-- exclude if in v2
QUALIFY ROW_NUMBER() OVER (PARTITION BY quiz_uuid, file_name ORDER BY COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) DESC) = 1