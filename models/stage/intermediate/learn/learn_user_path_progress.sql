{{ config(tags=["on_deploy"]) }}

WITH current_state AS (
    SELECT
         item:uuid::VARCHAR AS user_uuid
        ,value:uuid::VARCHAR AS path_uuid
        ,IFF(TRY_TO_TIMESTAMP(value:starting_date::VARCHAR) IS NULL, TRY_TO_TIMESTAMP(value:starting_date::VARCHAR, 'MM/DD/YYYY HH12:MI:SS AM'), TRY_TO_TIMESTAMP(value:starting_date::VARCHAR)) AS starting_date 
        ,IFF(TRY_TO_TIMESTAMP(value:completion_date::VARCHAR) IS NULL, TRY_TO_TIMESTAMP(value:completion_date::VARCHAR, 'MM/DD/YYYY HH12:MI:SS AM'), TRY_TO_TIMESTAMP(value:completion_date::VARCHAR)) AS completion_date 
        ,value:progress::DECIMAL(5,2) AS path_progress
        ,IFF(path_progress = 1, TRUE, FALSE) AS completed
        ,u.file_name
        ,COALESCE(load_timestamp, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
        ,2 AS learn_version
        ,COALESCE(op, 'L') AS op
    FROM repsites.learn_parquet.users AS u
        ,LATERAL FLATTEN(input => item:paths)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY value:uuid::VARCHAR, item:uuid::VARCHAR ORDER BY u.load_timestamp DESC, u.file_name DESC, u.file_row_number DESC) = 1
)

SELECT 
     user_uuid
    ,path_uuid
    ,starting_date 
    ,completion_date 
    ,path_progress
    ,completed
    ,file_name
    ,load_time
    ,learn_version
FROM current_state
WHERE op != 'D'

UNION ALL

SELECT
     REPLACE(SPLIT_PART(u.filename, '/', 4), '.json', '') AS user_uuid
    ,value:uuid::VARCHAR AS path_uuid
    ,value:starting_date::TIMESTAMP_NTZ AS starting_date 
    ,value:completion_date::TIMESTAMP_NTZ AS completion_date 
    ,value:progress::DECIMAL(5,2) AS path_progress
    ,IFF(path_progress = 1, TRUE, FALSE) AS completed
    ,u.filename AS file_name
    ,COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) AS load_time
    ,1 AS learn_version

FROM repsites.learn.raw_user_state AS u
    ,LATERAL FLATTEN(input => item:paths)
WHERE REPLACE(SPLIT_PART(u.filename, '/', 4), '.json', '') IN (SELECT user_uuid FROM {{ ref('v2_users') }} ) = False -- exclude if in v2
QUALIFY ROW_NUMBER() OVER (PARTITION BY path_uuid, file_name ORDER BY COALESCE(load_time::TIMESTAMP_NTZ, '2020-08-01'::TIMESTAMP_NTZ) DESC) = 1