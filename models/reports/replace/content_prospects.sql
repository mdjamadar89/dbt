{{ config(tags=["every_night"]) }}

SELECT 
        (CASE
            WHEN (internal_links.object:target::VARCHAR = 'twitter') THEN (((COUNT(internal_link_clicks.object:id::INT) - COUNT(internal_links.object:contact_id::INT)) + COUNT(DISTINCT internal_links.object:contact_id::INT)) * 0.05)
            ELSE ((COUNT(internal_link_clicks.object:id::INT) - COUNT(internal_links.object:contact_id::INT)) + COUNT(DISTINCT internal_links.object:contact_id::INT))
        END) AS prospects,
        internal_links.id AS internal_link_id,
        internal_links.object:slug::VARCHAR AS slug,
        (CASE
            WHEN (internal_links.object:controller::VARCHAR = 'links') THEN 'images'
            WHEN (internal_links.object:controller::VARCHAR = 'panels') THEN 'web_pages'
            ELSE internal_links.object:controller::VARCHAR
        END) AS controller,
        internal_links.object:target::VARCHAR AS target,
        internal_link_clicks.object:created::DATETIME AS view_created,
        internal_links.object:site_id::VARCHAR AS site_id
    FROM
        ((repsites.mysql_parquet_complete.internal_link_clicks
        JOIN repsites.mysql_parquet_complete.internal_links ON ((internal_link_clicks.object:internal_link_id::INT = internal_links.id))
            AND (internal_link_clicks.schema_name = internal_links.schema_name))
        JOIN repsites.mysql_parquet_complete.users ON ((internal_links.object:user_id::INT = users.id)
            AND (internal_links.schema_name = users.schema_name)))
    WHERE
        ((NOT ((users.object:email::VARCHAR LIKE '%@soundconcepts.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%myverb.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%verb.tech')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%@f3code.com')))
            AND (NOT ((users.object:email::VARCHAR LIKE '%test.com'))))
    GROUP BY internal_link_clicks.object:created::DATETIME , internal_links.object:slug::VARCHAR , internal_links.object:target::VARCHAR , internal_links.object:controller::VARCHAR , internal_links.id, internal_links.object:site_id::VARCHAR, internal_link_clicks.schema_name
    ORDER BY internal_links.object:site_id::VARCHAR, internal_links.id, internal_link_clicks.schema_name