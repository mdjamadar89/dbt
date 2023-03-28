{{ config(tags=["every_night"]) }}

SELECT 
        internal_link_clicks.id AS clicks_id,
        internal_links.id AS internal_link_id,
        internal_links.object:contact_id::INT AS contacts_id,
        internal_links.object:controller::VARCHAR AS controller,
        internal_links.object:slug::VARCHAR AS slug,
        internal_links.object:target::VARCHAR AS target,
        internal_link_clicks.object:created::DATETIME AS created,
        internal_links.object:site_id::VARCHAR AS site_id
    FROM
        ((repsites.mysql_parquet_complete.internal_link_clicks
        JOIN repsites.mysql_parquet_complete.internal_links ON ((internal_link_clicks.object:internal_link_id::INT = internal_links.id))
            AND internal_link_clicks.schema_name = internal_links.schema_name
        JOIN repsites.mysql_parquet_complete.users ON ((internal_links.object:user_id::INT = users.id))
            AND internal_links.schema_name = users.schema_name))
    ORDER BY internal_links.object:site_id::VARCHAR, internal_link_clicks.schema_name