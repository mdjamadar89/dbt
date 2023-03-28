{{ config(tags=["on_deploy"]) }}

WITH quizzes AS (
      SELECT
         item:uuid::VARCHAR AS user_uuid
        ,value:uuid::VARCHAR AS quiz_uuid
        ,value:answers AS answers
        ,u.file_name
        ,COALESCE(load_timestamp, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
        ,COALESCE(op, 'L') AS op
    FROM repsites.learn_parquet.users AS u
        ,LATERAL FLATTEN(input => item:quizzes)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY value:uuid::VARCHAR, item:uuid::VARCHAR ORDER BY u.load_timestamp DESC, u.file_name DESC, u.file_row_number DESC) = 1
)

SELECT
     q.user_uuid
    ,value:quiz_question_id::VARCHAR AS id
    ,value:quiz_question_id::VARCHAR AS quiz_question_id
    ,q.quiz_uuid
    ,COALESCE(value:user_selected::BOOLEAN, FALSE) AS user_selected
    ,q.file_name
    ,q.load_time
    ,2 AS learn_version
FROM quizzes AS q
    ,LATERAL FLATTEN(input => answers)
WHERE op != 'D'

UNION ALL

SELECT
     REPLACE(SPLIT_PART(u.filename, '/', 4), '.json', '') AS user_uuid
    ,value:id::VARCHAR AS id
    ,value:quiz_question_id::VARCHAR AS quiz_question_id
    ,value:quiz_uuid::VARCHAR AS quiz_uuid
    ,value:user_selected::VARCHAR AS user_selected
    ,u.filename AS file_name
    ,COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
    ,1 AS learn_version
FROM repsites.learn.raw_user_state AS u
        ,LATERAL FLATTEN(input => item:quiz_answers)
WHERE REPLACE(SPLIT_PART(u.filename, '/', 4), '.json', '') IN (SELECT user_uuid FROM {{ ref('v2_users') }} ) = False-- exclude if in v2
QUALIFY ROW_NUMBER() OVER (PARTITION BY quiz_question_id, quiz_uuid, file_name ORDER BY COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) DESC) = 1