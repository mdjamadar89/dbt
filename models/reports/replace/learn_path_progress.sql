{{ config(tags=["every_6_hours", "on_deploy"]) }}

SELECT 
    *
FROM {{ ref('learn_path_activity') }}
WHERE 
    CASE WHEN site_id = '133' AND path_title NOT ILIKE '%Kyäni%' THEN FALSE ELSE TRUE END = TRUE
    AND path_completed = TRUE