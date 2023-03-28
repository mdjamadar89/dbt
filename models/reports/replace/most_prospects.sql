{{ config(tags=["every_night"]) }}

SELECT 
        (CASE
            WHEN (internal_links.object:target::VARCHAR = 'twitter') THEN (((COUNT(internal_link_clicks.object:id::INT) - COUNT(internal_links.object:contact_id::INT)) + COUNT(DISTINCT internal_links.object:contact_id::INT)) * 0.05)
            ELSE ((COUNT(internal_link_clicks.object:id::INT) - COUNT(internal_links.object:contact_id::INT)) + COUNT(DISTINCT internal_links.object:contact_id::INT))
        END) AS prospects,
        internal_links.object:site_id::VARCHAR AS site_id,
        internal_links.id AS internal_link_id,
        users.object:first_name::VARCHAR AS first_name,
        users.object:last_name::VARCHAR AS last_name,
        users.object:username::VARCHAR AS username,
        users.id AS user_id,
        users.object:voffice_id::VARCHAR AS voffice_id,
        users.object:email::VARCHAR AS email,
        internal_link_clicks.object:created::DATETIME AS view_created
    FROM
        ((repsites.mysql_parquet_complete.internal_link_clicks
        JOIN repsites.mysql_parquet_complete.internal_links ON ((internal_link_clicks.object:internal_link_id::INT = internal_links.id)
            AND (internal_link_clicks.schema_name = internal_links.schema_name))
        JOIN repsites.mysql_parquet_complete.users ON ((internal_links.object:user_id::INT = users.id)
            AND internal_links.schema_name = users.schema_name)))
    WHERE
        ((NOT (users.object:email::VARCHAR LIKE '%@soundconcepts.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%myverb.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%verb.tech')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%@f3code.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%test.com')))
    GROUP BY internal_links.object:site_id::VARCHAR, internal_links.id, users.object:first_name::VARCHAR, users.object:last_name::VARCHAR, users.object:username::VARCHAR, users.id, users.object:voffice_id::VARCHAR, users.object:email::VARCHAR, internal_link_clicks.object:created::DATETIME, internal_links.object:target::VARCHAR, internal_link_clicks.schema_name
    ORDER BY internal_links.object:site_id::VARCHAR, internal_links.id, internal_link_clicks.schema_name