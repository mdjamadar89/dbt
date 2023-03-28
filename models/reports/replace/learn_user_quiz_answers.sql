{{ config(tags=["every_6_hours", "on_deploy"]) }}

SELECT DISTINCT
     qa.user_uuid
    ,qq.site_id
    ,ll.path_uuid
    ,ll.path_title
    ,ll.course_title
    ,ll.lesson_title
    ,qa.quiz_uuid
    ,qq.question
    ,qq.lesson_uuid
    ,qa.user_selected AS user_answer_correct
    ,qq.sort_order
    ,u.first_name AS user_first_name
    ,u.last_name AS user_last_name
    ,u.email AS user_email
    ,u.voffice_id
    ,u.created AS user_created_date
    ,u.first_user_activity_date
    ,u.user_id
    ,qa.file_name
    ,qa.load_time
FROM {{ ref('user_quiz_answers') }} AS qa
    LEFT JOIN {{ ref('learn_quiz_questions') }} AS qq
        ON qa.quiz_uuid = qq.uuid
       AND qa.quiz_question_id = qq.question_id
    LEFT JOIN {{ ref('learn_lessons') }} AS ll
        ON qq.lesson_uuid = ll.lesson_uuid
    LEFT JOIN {{ ref('users') }} AS u
        ON qa.user_uuid = u.user_uuid