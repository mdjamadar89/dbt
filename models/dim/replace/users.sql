{{ config(tags=["every_6_hours", "on_deploy"]) }}

SELECT DISTINCT
     MD5(u.id::VARCHAR || u.schema_name) AS user_key
    ,u.id::VARCHAR AS user_id
    ,IFF(u.object:modified::TIMESTAMP_NTZ = MAX(u.object:modified::TIMESTAMP_NTZ) OVER (PARTITION BY ucf.user_uuid, u.object:site_id::NUMBER(32,0)), ucf.user_uuid, NULL) AS user_uuid
    ,u.object:site_id::VARCHAR AS site_id
    ,u.object:referred_by::VARCHAR AS referred_by
    ,u.object:voffice_id::VARCHAR AS voffice_id
    ,u.object:username::VARCHAR AS username
    ,u.object:subscription_level::VARCHAR AS subscription_level
    ,u.object:first_name::VARCHAR AS first_name
    ,u.object:last_name::VARCHAR AS last_name
    ,u.object:phone::VARCHAR AS phone
    ,u.object:email::VARCHAR AS email
    ,u.object:display_name::VARCHAR AS display_name
    ,u.object:display_phone::VARCHAR AS display_phone
    ,u.object:display_email::VARCHAR AS display_email
    ,u.object:created::TIMESTAMP_NTZ AS created
    ,uad.first_user_activity_date
    ,uad.last_user_activity_date
    ,COALESCE(DATEDIFF('day', u.object:created::TIMESTAMP_NTZ, uad.last_user_activity_date), 0) AS days_as_active_user
    ,u.object:modified::TIMESTAMP_NTZ AS modified
    ,u.object:modified_by_user_id::NUMBER(32,0) AS modified_by_user_id
    ,u.object:title::VARCHAR AS title
    ,u.object:address1::VARCHAR AS address1
    ,u.object:address2::VARCHAR AS address2
    ,u.object:city::VARCHAR AS city
    ,u.object:state::VARCHAR AS state
    ,u.object:country::VARCHAR AS country
    ,u.object:has_logged_in::NUMBER(32,0) AS has_logged_in
    ,u.object:google_analytics_id::VARCHAR AS google_analytics_id
    ,u.object:timezone::VARCHAR AS timezone
    ,u.schema_name AS schema_name
    ,'Repsites' AS source
FROM repsites.mysql_parquet_complete.users AS u
    LEFT JOIN {{ ref('user_activity_dates') }} AS uad
        ON uad.user_key = MD5(u.id || u.schema_name)
    LEFT JOIN (SELECT object:user_id::NUMBER(20,0) AS user_id
                     ,schema_name
                     ,object:value::VARCHAR AS user_uuid
               FROM repsites.mysql_parquet_complete.user_custom_fields
               WHERE object:name::VARCHAR = 'user_uuid' ) AS ucf
        ON ucf.user_id = u.id
       AND ucf.schema_name = u.schema_name

-- Add in VerbTeams:
UNION ALL

SELECT 
     MD5(vtu.object:"_id"::VARCHAR || vtu.object:"company_data":"_id"::VARCHAR) AS user_key
    ,vtu.object:"_id"::VARCHAR AS user_id
    ,vtu.object:"_id"::VARCHAR AS user_uuid
    ,vtu.object:"company_data":"_id"::VARCHAR AS site_id
    ,NULL AS referred_by
    ,NULL AS voffice_id
    ,NULL AS username
    ,NULL AS subscription_level
    ,vtu.object:"first_name"::VARCHAR AS first_name
    ,vtu.object:"last_name"::VARCHAR AS last_name
    ,NULL AS phone
    ,vtu.object:"email"::VARCHAR AS email
    ,NULL AS display_name
    ,NULL AS display_phone
    ,NULL AS display_email
    ,NULL AS created
    ,NULL AS first_user_activity_date
    ,NULL AS last_user_activity_date
    ,NULL AS days_as_active_user
    ,NULL AS modified
    ,NULL AS modified_by_user_id
    ,NULL AS title
    ,NULL AS address1
    ,NULL AS address2
    ,NULL AS city
    ,NULL AS state
    ,NULL AS country
    ,NULL AS has_logged_in
    ,NULL AS google_analytics_id
    ,NULL AS timezone
    ,vtu.object:"company_data":name::VARCHAR AS schema_name
    ,'verbTEAMS' AS source
FROM repsites.verb_teams_parquet.users AS vtu