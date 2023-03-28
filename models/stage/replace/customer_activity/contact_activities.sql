{{ config(tags=["every_night"]) }}

SELECT contact_activities.object:created::TIMESTAMP_NTZ AS activity_timestamp
      ,MD5(contact_activities.object:user_id::VARCHAR || contact_activities.schema_name) AS user_key
      ,MD5(users.object:site_id::NUMBER(30, 0)) AS site_key
      ,NULL AS market_key
      ,'Contact Activities' AS activity_name
      ,COALESCE(contact_activities.object:"TRIGGER"::VARCHAR, 'Not Recorded') AS activity_type
      ,CONCAT('Message: ', COALESCE(contact_activities.object:message::VARCHAR, 'Not Recorded')
             ,', Trigger: ', COALESCE(contact_activities.object:"TRIGGER"::VARCHAR, '')
       ) AS activity_details
      ,'MySQL' AS source_datasource
      ,COALESCE(contact_activities.object:source::VARCHAR, 'Not Recorded') AS source_name

FROM repsites.mysql_parquet_complete.contact_activities
    LEFT JOIN repsites.mysql_parquet_complete.users
        ON users.id = contact_activities.object:user_id::NUMBER(30, 0)
       AND users.schema_name = contact_activities.schema_name
WHERE NOT (users.object:subscription_level::VARCHAR REGEXP 'system')
  AND NOT (users.object:referred_by::VARCHAR LIKE '%SC%')
  AND NOT (users.object:email::VARCHAR REGEXP '@soundconcepts.com|@myverb.com|@test.com|@f3code.com|@verb.tech')
