{{ config(tags=["every_6_hours", "on_deploy"]) }}

WITH events AS (
    SELECT
         object:event_type::VARCHAR AS event_type
        ,object:event_data:attendee_email::VARCHAR AS attendee_email
        ,object:event_data:attendee_first::VARCHAR AS attendee_first
        ,object:event_data:attendee_id::VARCHAR AS attendee_id
        ,object:event_data:attendee_initial::VARCHAR AS attendee_initial
        ,object:event_data:attendee_last::VARCHAR AS attendee_last
        ,object:event_data:client_id::VARCHAR AS client_id
        ,CASE 
            WHEN TRY_TO_TIMESTAMP(object:event_data:date::VARCHAR) IS NULL THEN TRY_TO_TIMESTAMP(object:event_data:date::VARCHAR, 'DY, DD MON YYYY HH24:MI:SS GMT')
            ELSE TRY_TO_TIMESTAMP(object:event_data:date::VARCHAR)
         END AS date
        ,object:event_data:elapsed_seconds::FLOAT AS elapsed_seconds
        ,object:event_data:interaction_action::VARCHAR AS interaction_action
        ,object:event_data:interaction_id::VARCHAR AS interaction_id
        ,object:event_data:interaction_imageUrl::VARCHAR AS interaction_imageUrl
        ,object:event_data:interaction_label::VARCHAR AS interaction_label
        ,object:event_data:interaction_type::VARCHAR AS interaction_type
        ,object:event_data:issuer_contact_id::VARCHAR AS issuer_contact_id
        ,object:event_data:issuer_referrer_id::VARCHAR AS issuer_referrer_id
        ,object:event_data:issuer_referrer_name::VARCHAR AS issuer_referrer_name
        ,object:event_data:stream_id::VARCHAR AS stream_id
        ,object:event_data:stream_instance_id::VARCHAR AS stream_instance_id
        ,object:event_data:user_id::VARCHAR AS user_id
        ,object:event_data:utcDate::TIMESTAMP_NTZ AS utcDate
        ,object:event_data:video_description::VARCHAR AS video_description
        ,object:event_data:video_id::VARCHAR AS video_id
        ,object:event_data:video_length::VARCHAR AS video_length
        ,object:event_data:video_title::VARCHAR AS video_title
        ,try_to_decimal(object:event_data:video_watch_progress::VARCHAR, 10, 2) AS video_watch_progress
        ,object:event_data:viewer_first::VARCHAR AS viewer_first
        ,object:event_data:viewer_id::VARCHAR AS viewer_id
        ,object:event_data:viewer_last::VARCHAR AS viewer_last
        -- ,object AS original_object
        ,file_name 
        ,file_row_number
        ,load_timestamp 
    FROM repsites.verb_live.verb_core_event_data
)

SELECT 
     MD5(ucf.user_id || ucf.schema_name) AS user_key
    ,MD5(u.site_id) AS site_key
    ,UPPER(ucf.schema_name) AS schema_name
    ,vled.* 
FROM events AS vled
    LEFT JOIN (SELECT DISTINCT object:user_id::VARCHAR AS user_id
                     ,schema_name
                     ,object:value::VARCHAR AS verb_live_stream_id
               FROM repsites.mysql_parquet_complete.user_custom_fields
               WHERE object:name::VARCHAR = 'verb_live_stream_id' ) AS ucf
        ON vled.stream_id = ucf.verb_live_stream_id
     LEFT JOIN {{ ref('users') }} AS u
        ON u.user_id = ucf.user_id
       AND u.schema_name = ucf.schema_name