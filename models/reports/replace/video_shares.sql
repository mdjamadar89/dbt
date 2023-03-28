{{ config(tags=["every_night"]) }}

SELECT 
        task_logs.object:id::INT AS share_id,
        internal_links.object:id::INT AS internal_link_id,
        videos.object:user_id::INT AS user_id,
        users.object:voffice_id::VARCHAR AS voffice_id,
        users.object:first_name::VARCHAR AS first_name,
        users.object:last_name::VARCHAR AS last_name,
        users.object:username::VARCHAR AS username,
        internal_links.object:contact_id::INT AS contact_id,
        internal_links.object:asset_id::VARCHAR AS asset_id,
        videos.object:title::VARCHAR AS title,
        internal_links.object:target::VARCHAR AS share_method,
        task_logs.object:created::DATETIME AS share_date,
        videos.object:length::VARCHAR AS length,
        videos.object:removed::INT AS removed,
        (CASE
            WHEN (internal_links.object:controller::VARCHAR = 'links') THEN 'images'
            WHEN (internal_links.object:controller::VARCHAR = 'panels') THEN 'web_pages'
            ELSE internal_links.object:controller::VARCHAR
        END) AS controller,
        internal_links.object:slug::VARCHAR AS slug,
        internal_links.object:site_id::VARCHAR AS site_id
    FROM
        repsites.mysql_parquet_complete.task_logs
        LEFT JOIN repsites.mysql_parquet_complete.internal_links ON (task_logs.object:internal_link_id::INT = internal_links.object:id::INT)
            AND task_logs.schema_name = internal_links.schema_name
        LEFT JOIN repsites.mysql_parquet_complete.users ON (task_logs.object:user_id::INT = users.object:id::INT)
            AND task_logs.schema_name = users.schema_name
        JOIN repsites.mysql_parquet_complete.videos ON (internal_links.object:asset_id::VARCHAR = (CONCAT('video_', videos.object:id::VARCHAR)))
            AND internal_links.schema_name = videos.schema_name
    WHERE
        ((NOT ((users.object:email::VARCHAR LIKE '%@soundconcepts.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%@myverb.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%@verb.tech')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%@f3code.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%test.com'))))
    ORDER BY internal_links.object:site_id::VARCHAR