{{ config(tags=["every_night"]) }}

SELECT 
  user_activities.object:user_id::INT AS user_id,
  users.object:voffice_id::VARCHAR AS voffice_id,
  users.object:first_name::VARCHAR AS first_name,
  users.object:last_name::VARCHAR AS last_name,
  users.object:username::VARCHAR AS username,
  users.object:email::VARCHAR AS email,
  (CASE
      WHEN (users.object:subscription_level::VARCHAR = '') THEN 'normal'
      ELSE users.object:subscription_level::VARCHAR
  END) AS subscription_level,
  users.object:created::DATETIME AS user_created,
  user_activities.object:id::INT AS activity_id,
  user_activities.object:activity_type::VARCHAR AS activity_type,
  user_activities.object:created::DATETIME AS activity_created,
  users.object:site_id::VARCHAR AS site_id
FROM
  (repsites.mysql_parquet_complete.user_activities
  JOIN repsites.mysql_parquet_complete.users ON ((user_activities.object:user_id::INT = users.object:id::INT))
    AND user_activities.schema_name = users.schema_name)
WHERE
  ((NOT ((users.object:email::VARCHAR LIKE '%@soundconcepts.com')))
      AND (NOT ((users.object:email::VARCHAR LIKE '%myverb.com')))
      AND (NOT ((users.object:email::VARCHAR LIKE '%verb.tech')))
      AND (NOT ((users.object:email::VARCHAR LIKE '%@f3code.com')))
      AND (NOT ((users.object:email::VARCHAR LIKE '%test.com'))))
ORDER BY users.object:site_id::VARCHAR, user_activities.object:user_id::INT, users.schema_name
