{{ config(tags=["every_6_hours", "on_deploy"]) }}

SELECT 
    COALESCE(lp.site_id, u.site_id)::VARCHAR AS site_id
    ,MD5(COALESCE(lp.site_id, u.site_id)::VARCHAR) AS site_key
    ,lupp.path_uuid
    ,lp.title AS path_title
    ,lp.description AS path_description
    ,lp.locale AS path_language
    ,lp.market_ids AS path_market_ids
    ,lp.markets AS path_markets
    ,lupp.starting_date AS path_starting_date
    ,lupp.completion_date AS path_completion_date
    ,lupp.completed AS path_completed
    ,lupp.path_progress
    ,lupp.user_uuid
    ,u.first_name AS user_first_name
    ,u.last_name AS user_last_name
    ,u.email AS user_email
    ,u.voffice_id
    ,u.user_id
    ,u.created AS user_created_date
    ,u.first_user_activity_date
    ,lupp.load_time

FROM {{ ref('learn_user_path_progress') }} AS lupp
    LEFT JOIN {{ ref('users') }} AS u
        ON REPLACE(lupp.user_uuid, '.json', '') = REPLACE(u.user_uuid, '.json', '')
    LEFT JOIN {{ ref('learn_paths') }} AS lp
        ON lupp.path_uuid = lp.uuid
WHERE lp.site_id = COALESCE(u.site_id, lp.site_id) -- exclude users with same UUID across sites