{{ config(tags=["on_deploy"]) }}

SELECT u.user_uuid
FROM {{ ref('users') }} AS u
    JOIN {{ ref('v2_sites') }} AS s
        ON s.site_id = u.site_id