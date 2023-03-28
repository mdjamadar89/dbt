{{ config(tags=["every_6_hours", "on_deploy"]) }}

WITH all_sites AS (
    SELECT DISTINCT 
         UPPER(table_schema) AS schema_name
        ,lower(table_name) AS table_name
    FROM REPSITES.MYSQL.INFORMATION_SCHEMA_TABLES_QUERY_RESULTS
    WHERE lower(table_name) IN (
        'user_custom_fields',
        'users',
        'video_views',
        'contacts',
        'internal_link_clicks',
        'markets',
        'user_activities',
        'videos',
        'internal_links',
        'task_logs',
        'contact_activities',
        'myship_orders',
        'sites'
    )
    AND DATEDIFF('day', TO_TIMESTAMP(__HEVO__INGESTED_AT, 3)::DATE, CURRENT_DATE) < 3
    AND UPPER(table_schema) != 'PERFORMANCE_SCHEMA'
    QUALIFY ROW_NUMBER() OVER(PARTITION BY table_schema, table_name ORDER BY TO_TIMESTAMP(__HEVO__INGESTED_AT, 3)::DATE DESC) = 1
), contacts AS (
    SELECT
         'contacts' AS table_name
        ,a.schema_name
        ,count(b.schema_name) AS count
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.contacts AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'contacts'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), contact_activities AS (
    SELECT
         'contact_activities' AS table_name
        ,a.schema_name
        ,count(b.schema_name) AS count
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.contact_activities AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'contact_activities'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), internal_links AS (
    SELECT
         'internal_links' AS table_name
        ,a.schema_name
        ,count(b.schema_name) AS count
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.internal_links AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'internal_links'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), internal_link_clicks AS (
    SELECT
         'internal_link_clicks' AS table_name
        ,a.schema_name
        ,count(b.schema_name) AS count
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.internal_link_clicks AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'internal_link_clicks'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), markets AS (
    SELECT
         'markets' AS table_name
        ,a.schema_name
        ,count(b.schema_name)
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.markets AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'markets'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), myship_orders AS (
    SELECT
         'myship_orders' AS table_name
        ,a.schema_name
        ,count(b.schema_name)
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.myship_orders AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'myship_orders'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), sites AS (
    SELECT
         'sites' AS table_name
        ,a.schema_name
        ,count(b.schema_name) AS count
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.sites AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'sites'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), task_logs AS (
    SELECT
         'task_logs' AS table_name
        ,a.schema_name
        ,count(b.schema_name) AS count
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.task_logs AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'task_logs'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), users AS (
    SELECT
         'users' AS table_name
        ,a.schema_name
        ,count(b.schema_name) AS count
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.users AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'users'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), user_activities AS (
    SELECT
         'user_activities' AS table_name
        ,a.schema_name
        ,count(b.schema_name) AS count
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.user_activities AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'user_activities'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), user_custom_fields AS (
    SELECT
         'user_custom_fields' AS table_name
        ,a.schema_name
        ,count(b.schema_name) AS count
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.user_custom_fields AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'user_custom_fields'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), videos AS (
    SELECT
         'videos' AS table_name
        ,a.schema_name
        ,count(b.schema_name) AS count
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.videos AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'videos'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), video_views AS (
    SELECT
         'video_views' AS table_name
        ,a.schema_name
        ,count(b.schema_name) AS count
    FROM all_sites AS a
        LEFT JOIN repsites.mysql_complete.video_views AS b
            ON a.schema_name = b.schema_name
           AND a.table_name = 'video_views'
    GROUP BY 1,2
    HAVING count(b.schema_name) = 0
), combine AS (
    SELECT * FROM contacts
    UNION ALL
    SELECT * FROM contact_activities
    UNION ALL
    SELECT * FROM internal_links
    UNION ALL
    SELECT * FROM internal_link_clicks
    UNION ALL
    SELECT * FROM markets
    UNION ALL
    SELECT * FROM myship_orders
    -- UNION ALL
    -- SELECT * FROM sites
    UNION ALL
    SELECT * FROM task_logs
    UNION ALL
    SELECT * FROM users
    UNION ALL
    SELECT * FROM user_activities
    UNION ALL
    SELECT * FROM user_custom_fields
    UNION ALL
    SELECT * FROM videos
    UNION ALL
    SELECT * FROM video_views
)

SELECT
     table_name
    ,schema_name
    ,count
FROM combine
WHERE table_name IS NOT NULL 
    AND schema_name NOT IN (
             'REPSITES_TEMPLATE'
            ,'TMP'
            ,'REPSITES_TEST'
            ,'REPSITES_SRINI_DEV'
            ,'REPSITES_VERBTESTING'
            ,'REPSITES_VERBHUB'
            ,'REPSITES_VERB')
ORDER BY 2,1