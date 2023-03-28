{{ config(tags=["every_night"]) }}

SELECT task_logs.object:created::TIMESTAMP_NTZ AS activity_timestamp
      ,MD5(task_logs.object:user_id::NUMBER(30, 0) || task_logs.schema_name) AS user_key
      ,MD5(users.object:site_id::NUMBER(30, 0)) AS site_key
      ,task_logs.object:market_id::NUMBER(30, 0) AS market_key
      ,'Task Logs' AS activity_name
      ,COALESCE(task_logs.object:"TRIGGER"::VARCHAR, 'Not Recorded') AS activity_type
      ,CONCAT('Task Name: ', COALESCE(task_logs.object:task_name::VARCHAR, '')
             ,', Trigger: ', COALESCE(task_logs.object:"TRIGGER"::VARCHAR, '')
       ) AS activity_details
      ,'MySQL' AS source_datasource
      ,COALESCE(task_logs.object:source::VARCHAR, 'Not Recorded') AS source_name

FROM repsites.mysql_parquet_complete.task_logs
    LEFT JOIN repsites.mysql_parquet_complete.users
        ON users.id = task_logs.object:user_id::NUMBER(30, 0)
       AND users.schema_name = task_logs.schema_name
WHERE NOT (users.object:subscription_level::VARCHAR REGEXP 'system')
  AND NOT (users.object:referred_by::VARCHAR LIKE '%SC%')
  AND NOT (users.object:email::VARCHAR REGEXP '@soundconcepts.com|@myverb.com|@test.com|@f3code.com|@verb.tech')