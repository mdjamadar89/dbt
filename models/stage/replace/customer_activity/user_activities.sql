{{ config(tags=["every_night"]) }}

SELECT user_activities.created AS activity_timestamp
      ,MD5(user_activities.user_id || user_activities.schema_name) AS user_key
      ,MD5(users.site_id) AS site_key
      ,NULL AS market_key
      ,'User Activities' AS activity_name
      ,COALESCE(user_activities.activity_type, 'Not Recorded') AS activity_type
      ,NULL AS activity_details
      ,'MySQL' AS source_datasource
      ,COALESCE(user_activities.source, 'Not Recorded') AS source_name
FROM repsites.mysql_complete.user_activities
    LEFT JOIN repsites.mysql_complete.users
        ON users.id = user_activities.user_id
       AND users.schema_name = user_activities.schema_name
WHERE NOT (users.subscription_level REGEXP 'system')
  AND NOT (users.referred_by LIKE '%SC%')
  AND NOT (users.email REGEXP '@soundconcepts.com|@myverb.com|@test.com|@f3code.com|@verb.tech')