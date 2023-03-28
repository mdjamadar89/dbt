{{ config(tags=["every_6_hours", "on_deploy"]) }}

SELECT 
        users.object:id::INT AS user_id,
        users.object:voffice_id::VARCHAR AS voffice_id,
        users.object:username::VARCHAR AS username,
        users.object:first_name::VARCHAR AS first_name,
        users.object:last_name::VARCHAR AS last_name,
        users.object:email::VARCHAR AS email,
        (CASE
            WHEN (users.object:subscription_level::VARCHAR = ' ') THEN 'normal'
            ELSE users.object:subscription_level::VARCHAR
        END) AS subscription_level,
        users.object:created::DATETIME AS user_created,
        users.object:site_id::VARCHAR AS site_id
    FROM
        repsites.mysql_parquet_complete.users
    WHERE
        ((NOT ((users.object:email::VARCHAR LIKE '%@soundconcepts.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%myverb.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%verb.tech')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%@f3code.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%test.com'))))
    ORDER BY users.object:site_id::VARCHAR, users.object:id::INT