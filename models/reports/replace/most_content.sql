{{ config(tags=["every_night"]) }}

SELECT 
        task_logs.id AS task_id,
        (CASE
            WHEN (internal_links.object:controller::VARCHAR = 'links') THEN 'images'
            WHEN (internal_links.object:controller::VARCHAR = 'panels') THEN 'web_pages'
            ELSE internal_links.object:controller::VARCHAR
        END) AS controller,
        internal_links.object:slug::VARCHAR AS slug,
        task_logs.object:trigger::VARCHAR AS "TRIGGER",
        task_logs.object:internal_link_id::INT AS internal_link_id,
        task_logs.object:created::DATETIME AS task_created,
        users.object:site_id::VARCHAR AS site_id
    FROM
        ((repsites.mysql_parquet_complete.task_logs
        JOIN repsites.mysql_parquet_complete.users ON ((task_logs.object:user_id::INT = users.id))
            AND (task_logs.schema_name = users.schema_name))
        JOIN repsites.mysql_parquet_complete.internal_links ON ((task_logs.object:internal_link_id::INT = internal_links.id)
            AND (task_logs.schema_name = internal_links.schema_name)))
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
    ORDER BY users.object:site_id::VARCHAR, task_logs.id, task_logs.schema_name