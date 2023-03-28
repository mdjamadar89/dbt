{{ config(tags=["every_6_hours", "on_deploy"]) }}

WITH user_quizzes AS (
    SELECT
         user_uuid
        ,lesson_uuid
        ,SUM(IFF(passed = FALSE, 1, 0)) AS quizzed_failed
        ,SUM(IFF(passed = TRUE, 1, 0)) AS quizzed_passed
    FROM {{ ref('learn_user_quiz_progress') }} AS u
    GROUP BY 1,2    
), quiz_answers AS (
    SELECT 
         user_uuid
        ,site_id
        ,lesson_uuid
        ,SUM(IFF(user_answer_correct, 1, 0)) AS correct_answers
        ,COUNT(*) AS total_questions
    FROM {{ ref('learn_user_quiz_answers') }} AS qa
    GROUP BY 1,2,3
), number_of_questions AS (
    SELECT 
        lesson_uuid
        ,count(*) AS total_questions
    FROM  {{ ref('learn_quizzes') }}
        ,LATERAL FLATTEN(input => quiz_questions)
    GROUP BY 1
)

SELECT
     REPLACE(p.user_uuid, '.json', '') AS user_uuid
    ,MD5(l.lesson_uuid || l.site_id) AS learn_lesson_key
    ,l.site_id
    ,COALESCE(u.completed, FALSE) AS completed
    ,COALESCE(u.progress, 0) AS progress
    ,u.starting_date
    ,u.completion_date
    ,COALESCE(q.quizzed_failed, 0) AS quizzed_failed
    ,IFF(quiz_answers.correct_answers = num.total_questions 
        AND COALESCE(q.quizzed_passed, 0) = 0 
        AND quiz_answers.correct_answers > 0
        ,1, COALESCE(q.quizzed_passed, 0)) AS quizzed_passed
    ,COALESCE(quiz_answers.correct_answers, 0) AS total_correct_quiz_answers
    ,COALESCE(num.total_questions, 0) AS total_quiz_questions
    ,total_correct_quiz_answers / NULLIF(total_quiz_questions,0) AS percent_correct
    ,COALESCE(u.load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
FROM {{ ref('learn_user_path_progress') }} AS p
    LEFT JOIN {{ ref('users') }} AS users
        ON p.user_uuid = users.user_uuid
    LEFT JOIN {{ ref('learn_lessons') }} AS l
        ON p.path_uuid = l.path_uuid
    LEFT JOIN {{ ref('learn_user_lessons') }} AS u 
        ON p.user_uuid = u.user_uuid
       AND u.lesson_uuid = l.lesson_uuid
    LEFT JOIN user_quizzes AS q
        ON q.lesson_uuid = u.lesson_uuid
       AND q.user_uuid = p.user_uuid
    LEFT JOIN quiz_answers
        ON quiz_answers.user_uuid = p.user_uuid
       AND quiz_answers.site_id = l.site_id
       AND quiz_answers.lesson_uuid = l.lesson_uuid
    LEFT JOIN number_of_questions AS num
        ON num.lesson_uuid = l.lesson_uuid
WHERE l.site_id = COALESCE(users.site_id, l.site_id) -- exclude users with same UUID across sites