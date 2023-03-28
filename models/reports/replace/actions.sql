{{ config(tags=["every_night"]) }}

SELECT 
        task_logs.object:id::INT AS task_id,
        task_logs.object:user_id::INT AS user_id,
        users.object:voffice_id::VARCHAR AS voffice_id,
        users.object:username::VARCHAR AS username,
        users.object:site_id::VARCHAR AS site_id,
        task_logs.object:trigger::VARCHAR AS "TRIGGER",
        task_logs.object:internal_link_id::INT AS internal_link_id,
        task_logs.object:source::VARCHAR AS source,
        task_logs.object:created::DATETIME AS created
    FROM
        (repsites.mysql_parquet_complete.task_logs
        JOIN repsites.mysql_parquet_complete.users ON ((task_logs.object:user_id::INT = users.object:id::INT))
            AND task_logs.schema_name = users.schema_name)
    WHERE
        ((NOT ((users.object:email::VARCHAR LIKE '%@soundconcepts.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%myverb.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%verb.tech')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%@f3code.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%test.com'))))
    ORDER BY users.object:site_id::INT, task_logs.object:id::INT, task_logs.schema_name