{{ config(tags=["every_6_hours", "on_deploy"]) }}

SELECT 
    COALESCE(lp.site_id, u.site_id)::VARCHAR AS site_id
    ,MD5(COALESCE(lp.site_id, u.site_id)::VARCHAR) AS site_key
    ,lp.title AS path_title
    ,lp.description AS path_description
    ,lp.locale AS path_language
    ,lp.market_ids AS path_market_ids
    ,lp.markets AS path_markets
    ,lc.title AS course_title
    ,lc.description AS course_description
    ,lupp.starting_date AS course_starting_date
    ,lupp.completion_date AS course_completion_date
    ,lupp.completed AS course_completed
    ,lupp.course_progress
    ,lupp.user_uuid
    ,u.first_name AS user_first_name
    ,u.last_name AS user_last_name
    ,u.email AS user_email
    ,u.voffice_id
    ,u.created AS user_created_date
    ,u.first_user_activity_date
    ,u.user_id
    ,lupp.load_time AS load_time
FROM {{ ref('learn_user_course_progress') }} AS lupp
    LEFT JOIN {{ ref('users') }} AS u
        ON REPLACE(lupp.user_uuid, '.json', '') = REPLACE(u.user_uuid, '.json', '')
    LEFT JOIN {{ ref('learn_paths') }} AS lp
        ON lupp.path_uuid = lp.uuid
    LEFT JOIN {{ ref('learn_courses') }} AS lc
        ON lupp.course_uuid = lc.uuid
WHERE lp.site_id = COALESCE(u.site_id, lp.site_id) -- exclude users with same UUID across sites