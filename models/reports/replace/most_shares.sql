{{ config(tags=["every_night"]) }}

SELECT 
        task_logs.object:id::INT AS task_id,
        task_logs.object:internal_link_id::INT AS internal_link_id,
        users.object:site_id::VARCHAR AS site_id,
        users.object:first_name::VARCHAR AS first_name,
        users.object:last_name::VARCHAR AS last_name,
        users.object:username::VARCHAR AS username,
        users.object:voffice_id::VARCHAR AS voffice_id,
        users.object:email::VARCHAR AS email,
        task_logs.object:created::DATETIME AS link_created,
        task_logs.object:source::VARCHAR AS source
    FROM
        (repsites.mysql_parquet_complete.task_logs
        JOIN repsites.mysql_parquet_complete.users ON ((task_logs.object:user_id::INT = users.object:id::INT))
            AND (task_logs.schema_name = users.schema_name))
    WHERE
        ((task_logs.object:trigger::VARCHAR <> 'qr_code_label')
            AND (task_logs.object:trigger::VARCHAR <> 'contact_added')
            AND (task_logs.object:trigger::VARCHAR <> 'todo_added')
            AND (NOT ((task_logs.object:trigger::VARCHAR LIKE 'preview%')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%@soundconcepts.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%myverb.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%verb.tech')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%@f3code.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%test.com'))))