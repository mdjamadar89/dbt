{{ config(tags=["on_deploy"]) }}

SELECT
     q.uuid
    ,q.site_id
    ,q.title
    ,q.lesson_uuid
    ,q.award_thresholds
    ,value:uuid::VARCHAR AS question_id
    ,value:question::VARCHAR AS question
    ,value:quiz_answers AS quiz_answers
    ,value:sort_order::NUMBER(5,0) AS sort_order
    ,q.file_name
    ,q.load_time
    ,q.learn_version
FROM {{ ref('learn_quizzes') }} AS q
    ,LATERAL FLATTEN(input => quiz_questions)
ORDER BY q.uuid, sort_order