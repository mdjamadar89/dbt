{{ config(tags=["every_night"]) }}

SELECT myship_orders.object:created::TIMESTAMP_NTZ AS activity_timestamp
      ,MD5(myship_orders.object:user_id || myship_orders.schema_name) AS user_key
      ,MD5(users.object:site_id) AS site_key
      ,NULL AS market_key
      ,'MyShip Orders' AS activity_name
      ,'MyShip Orders' AS activity_type
      ,CONCAT('Contact ID: ', COALESCE(myship_orders.object:contact_id::VARCHAR, '')
             ,', Ship Tracking: ',  COALESCE(myship_orders.object:ship_tracking::VARCHAR, '')
             ,', Ship Date: ', COALESCE(myship_orders.object:ship_date::VARCHAR, '')
             ,', Ship USPS Response: ', COALESCE(myship_orders.object:ship_usps_response::VARCHAR, '')
       ) AS activity_details
      ,'MySQL' AS source_datasource
      ,'Not Recorded' AS source_name
FROM repsites.mysql_parquet_complete.myship_orders
    LEFT JOIN repsites.mysql_parquet_complete.users
        ON users.id = myship_orders.object:user_id::NUMBER(30, 0)
       AND users.schema_name = myship_orders.schema_name
WHERE NOT (users.object:subscription_level::VARCHAR REGEXP 'system')
  AND NOT (users.object:referred_by::VARCHAR LIKE '%SC%')
  AND NOT (users.object:email::VARCHAR REGEXP '@soundconcepts.com|@myverb.com|@test.com|@f3code.com|@verb.tech')