-- This does not run in dbt but this is the best place to save this for future reference

-- Creates the info_schema in snowflake as a table because we were running into 
-- issues access "too much data" from the info_schema. the table fixes the problem
CREATE OR REPLACE PROCEDURE build_info_schema_columns()
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$
  var result = "";
  try {
    var all_tables_cmd = `SELECT 
                               t1.table_schema
                              ,t1.table_name
                              ,t2.table_names
                          FROM repsites.information_schema.tables AS t1
                              JOIN data_warehouse.stage.table_names AS t2
                                  ON UPPER(t1.table_name) LIKE '%' || UPPER(t2.table_names)
                          WHERE t1.table_schema IN ('MYSQL', 'MYSQL_STANDALONE')`;
    var all_tables_stmt = snowflake.createStatement( {sqlText: all_tables_cmd} );
    var all_tables = all_tables_stmt.execute();

    // Loop through the tables, processing one tables at a time: 
    while (all_tables.next())  {
        var table = all_tables.getColumnValue(2);
        var schema = all_tables.getColumnValue(1);
        
        // Get all columns for the table
        var insert_column_cmd = `INSERT INTO mysql_complete.information_schema_columns
                          SELECT
                               c.table_schema
                              ,c.table_name 
                              ,c.COLUMN_NAME
                              ,c.ordinal_position
                          FROM repsites.information_schema.columns AS c
                          WHERE c.table_schema IN ('` + schema + `')
                            AND c.table_name = '` + table + `'
                          ORDER BY ordinal_position`;
        var insert_column_stmt = snowflake.createStatement( {sqlText: insert_column_cmd} );
        var insert_columns_data = insert_column_stmt.execute();
    }
    result = "Complete!";

  }
  catch (err)  {
    result =  err.message + " | " + err.stackTraceTxt;
  }
  return result;
  $$
;


-- Iterates through the tables and columns to create massive combined views
CREATE OR REPLACE PROCEDURE build_complete_mysql()
  RETURNS VARCHAR
  LANGUAGE JAVASCRIPT
  AS
  $$
  var result = "";
  try {
    var all_tables_cmd = `SELECT 
                              UPPER(table_names) AS table_name
                          FROM data_warehouse.stage.table_names`;
    var all_tables_stmt = snowflake.createStatement( {sqlText: all_tables_cmd} );
    var all_tables = all_tables_stmt.execute();

    // Loop through the tables, processing one tables at a time: 
    var results = [];
    while (all_tables.next())  {
        var view = '';
        var table = all_tables.getColumnValue(1);
        
        // Get all columns for the table
        var column_cmd = `WITH table_join AS (
                              SELECT 
                                   UPPER(table_schema || '_' || table_name) AS new_table_name
                                  ,UPPER(table_schema) AS table_schema
                                  ,UPPER(table_name) AS table_name
                              FROM repsites.mysql.information_schema_tables_query_results
                          )
                          
                          SELECT COLUMN_NAME, MIN(ordinal_position) AS min_ordinal_position, COUNT(*) AS count
                          FROM table_join AS t 
                              LEFT JOIN mysql_complete.information_schema_columns AS c
                                  ON t.new_table_name = c.table_name
                          WHERE c.table_schema IN ('MYSQL', 'MYSQL_STANDALONE')
                            AND t.table_name = '` + table + `'
                            AND UPPER(COLUMN_NAME) NOT LIKE ('%_HEVO%')
                          GROUP BY 1
                          ORDER BY min_ordinal_position, count DESC`;
        var column_stmt = snowflake.createStatement( {sqlText: column_cmd} );
        var columns_data = column_stmt.execute();
        var columns_data_array = [];
        while (columns_data.next())  {
            columns_data_array.push(columns_data.getColumnValue(1));
        }
        
        var tables_w_schema_cmd = `WITH table_join AS (
                                       SELECT 
                                            UPPER(table_schema || '_' || table_name) AS new_table_name
                                           ,UPPER(table_schema) AS table_schema
                                           ,UPPER(table_name) AS table_name
                                       FROM repsites.mysql.information_schema_tables_query_results
                                       WHERE UPPER(table_name) = '` + table + `'
                                   )
                                   
                                   SELECT t1.table_name, t2.table_schema, t1.table_schema AS snowflake_schema
                                   FROM mysql_complete.information_schema_tables AS t1
                                       INNER JOIN table_join AS t2
                                           ON t1.table_name = t2.new_table_name
                                   WHERE t2.table_name = '` + table + `'
                                     AND t1.table_schema IN ('MYSQL', 'MYSQL_STANDALONE')
                                   QUALIFY ROW_NUMBER() OVER(PARTITION BY t1.table_name ORDER BY t1.table_schema DESC) = 1`;    
        var tables_w_schema_stmt = snowflake.createStatement( {sqlText: tables_w_schema_cmd} );
        var tables_w_schema = tables_w_schema_stmt.execute();

        // Make UNION statement for table for all schemas
        while (tables_w_schema.next())  {
            var schema = tables_w_schema.getColumnValue(2);
            var schema_table = tables_w_schema.getColumnValue(1);
            var snowflake_schema = tables_w_schema.getColumnValue(3);

            // Get columns for specific table then build out specific columns
            var specific_columns_cmd = `SELECT COLUMN_NAME
                                            FROM mysql_complete.information_schema_columns AS c
                                            WHERE c.table_schema = '` + snowflake_schema + `'
                                              AND c.table_name = '` + schema_table + `'
                                              AND UPPER(COLUMN_NAME) NOT LIKE ('%_HEVO%')
                                            ORDER BY c.ordinal_position`
            var specific_columns_stmt = snowflake.createStatement( {sqlText: specific_columns_cmd} );
            var specific_columns = specific_columns_stmt.execute();
            var specific_columns_array = [];
            while (specific_columns.next())  {
                specific_columns_array.push(specific_columns.getColumnValue(1));
            }
            
            var columns = ''
            for (x in columns_data_array)  {
                var column = columns_data_array[x];
                if (specific_columns_array.includes(column)) {
                    columns = columns + ', "' + column + '"';
                } else {
                    columns = columns + ', NULL AS "' + column + '"';
                }
            }
            columns = columns.substring(2);

            view = view + `SELECT '` + schema + `' AS schema_name, ` + columns + ` FROM ` + snowflake_schema + `.` + schema_table + ` UNION ALL `;
        }
        
        // Remove last UNION ALL
        view = view.substring(0, view.length - 11);
        var view_cmd = `CREATE OR REPLACE VIEW mysql_complete.` + table + ` AS ` + view;
        var view_stmt = snowflake.createStatement( {sqlText: view_cmd} );
        var view_execute = view_stmt.execute();
        results.push(table);
    }

    result = results.toString();

  }
  catch (err)  {
    result =  err.message + " | " + err.stackTraceTxt + " | " + column + + view;
  }
  return result;
  $$
;

-- Builds the Tasks
CREATE OR REPLACE TASK mysql_complete.truncate_info_schema_columns_task
  WAREHOUSE = prod_wh
  SCHEDULE = 'USING CRON 0 23 * * 0 America/Los_Angeles'
AS
    TRUNCATE TABLE mysql_complete.information_schema_columns;

CREATE OR REPLACE TASK mysql_complete.build_info_schema_columns_task
  WAREHOUSE = prod_wh
  AFTER mysql_complete.truncate_info_schema_columns_task
AS
    CALL mysql_complete.build_info_schema_columns();

CREATE OR REPLACE TASK mysql_complete.reorder_info_schema_columns_task
  WAREHOUSE = prod_wh
  AFTER mysql_complete.build_info_schema_columns_task
AS
    CREATE OR REPLACE TABLE mysql_complete.information_schema_columns AS
    SELECT *
    FROM mysql_complete.information_schema_columns
    ORDER BY 1, 2, 4
;

-- Turns on the tasks in the correct order
CREATE OR REPLACE TASK mysql_complete.build_info_schema_tables_task
  WAREHOUSE = prod_wh
  AFTER mysql_complete.reorder_info_schema_columns_task
AS
    CREATE OR REPLACE TABLE mysql_complete.information_schema_tables AS
        SELECT t1.table_schema, t1.table_name
        FROM repsites.information_schema.tables AS t1
        WHERE t1.table_schema IN ('MYSQL', 'MYSQL_STANDALONE')
        ORDER BY 1, 2
;

CREATE OR REPLACE TASK mysql_complete.build_complete_mysql_task
  WAREHOUSE = prod_wh
  AFTER mysql_complete.build_info_schema_tables_task
AS
    CALL mysql_complete.build_complete_mysql();
    
ALTER TASK mysql_complete.build_complete_mysql_task RESUME;
ALTER TASK mysql_complete.build_info_schema_tables_task RESUME;
ALTER TASK mysql_complete.reorder_info_schema_columns_task RESUME;
ALTER TASK mysql_complete.build_info_schema_columns_task RESUME;
ALTER TASK mysql_complete.truncate_info_schema_columns_task RESUME;