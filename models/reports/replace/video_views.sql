{{ config(tags=["every_night"]) }}

SELECT 
        video_views.object:id::INT AS view_id,
        videos.object:user_id::INT AS user_id,
        users.object:voffice_id::VARCHAR AS voffice_id,
        users.object:first_name::VARCHAR AS first_name,
        users.object:last_name::VARCHAR AS last_name,
        users.object:username::VARCHAR AS username,
        users.object:email::VARCHAR AS email,
        videos.object:site_id::VARCHAR AS site_id,
        video_views.object:created::DATETIME AS view_date,
        videos.object:created::DATETIME AS video_created,
        videos.object:title::VARCHAR AS title,
        CONCAT('video_', videos.object:id::INT) AS video_id,
        videos.object:length::VARCHAR AS length,
        videos.object:removed::VARCHAR AS removed
    FROM
        ((repsites.mysql_parquet_complete.video_views
        JOIN repsites.mysql_parquet_complete.videos ON ((video_views.object:video_id::INT = videos.object:id::INT))
            AND (video_views.schema_name = videos.schema_name))
        LEFT JOIN repsites.mysql_parquet_complete.users ON ((videos.object:user_id::INT = users.object:id::INT))
            AND (videos.schema_name = users.schema_name))
   ORDER BY videos.object:site_id::VARCHAR, videos.object:user_id::INT, video_views.schema_name