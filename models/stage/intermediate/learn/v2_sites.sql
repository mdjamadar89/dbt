{{ config(tags=["on_deploy"]) }}

SELECT DISTINCT 
    COALESCE(item:site_id_str::VARCHAR,item:site_id::VARCHAR) AS site_id
FROM repsites.learn_parquet.path
    ,LATERAL FLATTEN(input => item:Paths)