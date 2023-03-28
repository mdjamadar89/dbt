{{ config(tags=["every_6_hours", "on_deploy"]) }}

SELECT
     MD5(l.uuid || p.site_id) AS learn_lesson_key
    ,p.uuid AS path_uuid
    ,p.site_id
    ,p.title AS path_title
    ,p.description AS path_description
    ,p.locale AS path_language
    ,p.market_ids AS path_market_ids
    ,p.markets AS path_markets
    ,c.uuid AS course_uuid
    ,c.title AS course_title
    ,c.description AS course_description
    ,c.locked AS course_is_locked
    ,c.sort_order AS course_sort_order
    ,l.uuid AS lesson_uuid
    ,l.title AS lesson_title
    ,l.description AS lesson_description
    ,l.completion_time AS lesson_completion_time
    ,l.sort_order AS lesson_sort_order
    ,p.learn_version
    ,COUNT(q.uuid) AS number_of_quizzes
FROM {{ ref('learn_paths') }} AS p
    LEFT JOIN {{ ref('learn_courses') }} AS c
        ON p.uuid = c.path_uuid
       AND p.site_id = c.site_id
    LEFT JOIN {{ ref('learn_lessons_stage') }} AS l
        ON c.uuid = l.course_uuid
       AND c.site_id = l.site_id
    LEFT JOIN {{ ref('learn_quizzes') }} AS q
        ON l.uuid = q.lesson_uuid
       AND l.site_id = q.site_id
WHERE l.uuid IS NOT NULL
{{ group_by(19) }}
