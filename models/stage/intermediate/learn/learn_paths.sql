{{ config(tags=["on_deploy"]) }}

WITH path_markets AS (
    SELECT DISTINCT
         uuid
        ,site_id
        ,value::INTEGER AS market_id
    FROM {{ ref('learn_paths_step_1') }}
        ,LATERAL FLATTEN(input => markets)
), market_names AS (
    SELECT 
         pm.uuid
        ,pm.site_id::VARCHAR AS site_id
        ,ARRAY_AGG(DISTINCT COALESCE(m.name, 'Default')) WITHIN GROUP (ORDER BY COALESCE(m.name, 'Default') ASC) AS market_name
    FROM path_markets AS pm
        LEFT JOIN {{ ref('markets') }} AS m
            ON pm.site_id = m.site_id
           AND pm.market_id = m.market_id
    GROUP BY 1,2
)

SELECT  
     lp.course_number
    ,lp.uuid
    ,lp.site_id
    ,lp.description
    ,lp.image_url
    ,lp.locale
    ,lp.markets AS market_ids
    ,mn.market_name AS markets
    ,lp.sort_order
    ,lp.title
    ,lp.file_name
    ,lp.load_time
    ,lp.learn_version

FROM {{ ref('learn_paths_step_1') }} AS lp
    LEFT JOIN market_names AS mn
        ON mn.uuid = lp.uuid
       AND mn.site_id = lp.site_id