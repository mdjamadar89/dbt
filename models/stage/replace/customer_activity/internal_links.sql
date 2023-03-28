{{ config(tags=["every_night"]) }}

SELECT internal_links.object:created::TIMESTAMP_NTZ AS activity_timestamp
      ,MD5(internal_links.object:user_id::VARCHAR || internal_links.schema_name) AS user_key
      ,MD5(users.object:site_id::NUMBER(30,0)) AS site_key
      ,internal_links.object:market_id::NUMBER(30,0) AS market_key
      ,'Internal Links' AS activity_name
      ,COALESCE(internal_links.object:target::VARCHAR, 'Not Recorded') AS activity_type
      ,CONCAT('Contact ID: ', COALESCE(internal_links.object:contact_id::VARCHAR, '')
             ,', Target: ', COALESCE(internal_links.object:target::VARCHAR, '')
             ,', Controller: ', COALESCE(internal_links.object:controller::VARCHAR, '')
             ,', Asset ID: ', COALESCE(internal_links.object:asset_id::VARCHAR, '')
       ) AS activity_details
      ,'MySQL' AS source_datasource
      ,COALESCE(internal_links.object:source::VARCHAR, 'Not Recorded') AS source_name

FROM repsites.mysql_parquet_complete.internal_links
    LEFT JOIN repsites.mysql_parquet_complete.users
        ON users.id = internal_links.object:user_id::NUMBER(30, 0)
       AND users.schema_name = internal_links.schema_name
WHERE NOT (users.object:subscription_level::VARCHAR REGEXP 'system')
  AND NOT (users.object:referred_by::VARCHAR LIKE '%SC%')
  AND NOT (users.object:email::VARCHAR REGEXP '@soundconcepts.com|@myverb.com|@test.com|@f3code.com|@verb.tech')