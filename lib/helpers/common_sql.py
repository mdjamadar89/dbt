import datetime

class CommonSQL():
    def __init__(self):
        self.column_names = ["t.$1", "t.$2", "t.$3", "t.$4", "t.$5", "t.$6", "t.$7", "t.$8", "t.$9", "t.$10",
                             "t.$11","t.$12","t.$13","t.$14","t.$15","t.$16","t.$17","t.$18","t.$19","t.$20",
                             "t.$21","t.$22","t.$23","t.$24","t.$25","t.$26","t.$27","t.$28","t.$29","t.$30",
                             "t.$31","t.$32","t.$33","t.$34","t.$35","t.$36","t.$37","t.$38","t.$39","t.$40"]

    def create_copy_stmt(self, s3_database, schema_name, table_name, number_of_columns):
        columns = ",".join(self.column_names[:number_of_columns])
        stmt = f"""
            COPY INTO {schema_name}.{table_name}
            FROM (SELECT {columns},
                         metadata$filename, metadata$file_row_number, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                  FROM @divvy_data_lake/{s3_database}/public/{table_name} t)
                FILE_FORMAT = (FORMAT_NAME = default_csv_format)
            ;
        """
        return stmt

    def create_most_recent_view_statement(self, from_schema, to_schema_name, table_name):
        stmt = f"""
            CREATE OR REPLACE VIEW {to_schema_name}.{table_name} AS
                WITH most_recent AS (
                    SELECT t1.*
                          ,ROW_NUMBER () OVER (PARTITION BY t1.id ORDER BY dw_import_filename DESC, dw_import_file_row_number DESC) as dw_most_recent_update
                    FROM {from_schema}.{table_name} t1
                )
                SELECT * FROM most_recent WHERE dw_most_recent_update = 1 AND op != 'D'
            ;
        """
        print(stmt)
        return stmt

    def create_full_copy_stmt(self, stage, s3_directory, schema_name, table_name):
        stmt = f"""
          COPY INTO {schema_name}.{table_name}
          FROM @{stage}/{s3_directory}
             FILE_FORMAT = (FORMAT_NAME = default_csv_format)
          ;
        """
        return stmt

    def create_insert_from_table(self, schema_name, into_table_name, from_table_name, column_names):
        columns = ",".join(column_names)
        stmt = f"""
            INSERT INTO {schema_name}.{into_table_name} ({columns})
            SELECT {columns} FROM {schema_name}.{from_table_name};
        """
        return stmt

    def truncate_table(self, schema_name, table_name):
        stmt = f"""
            TRUNCATE TABLE {schema_name}.{table_name};
        """
        return stmt

    def grant_usage_and_select(self, schema_name, to_role):
        stmts = [f"GRANT USAGE ON SCHEMA {schema_name} TO ROLE {to_role};",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema_name} TO ROLE {to_role};",
        f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {schema_name} TO ROLE {to_role};",
        f"GRANT SELECT ON ALL VIEWS IN SCHEMA {schema_name} TO ROLE {to_role};",
        f"GRANT SELECT ON FUTURE VIEWS IN SCHEMA {schema_name} TO ROLE {to_role};"]

        return stmts

    def get_databases(self):
        stmt = f"""
            SHOW DATABASES;
        """
        return stmt

    def get_drop_database_stmts(self, databases, days_to_delete, connection):
        stmts = []
        for r in databases:
            role = r[5]
            database_name = r[1]

            if role == 'DEV_ROLE' and database_name not in ('DATA_WAREHOUSE_TEST', 'DATA_LAKE_TEST'):
                last_query = f"SELECT COUNT(*) FROM snowflake.account_usage.query_history WHERE database_name = '{database_name}' AND DATEDIFF('day', start_time, current_date) < {days_to_delete};"
                # print(last_query)
                cur = connection.cursor()
                last_query = cur.execute(last_query)
                for response in last_query:
                    print(f"{database_name} : {response[0]}")
                    if response[0] == 0:
                        stmt = f"DROP DATABASE {database_name};"
                        stmts.append(stmt)
                cur.close()
        return stmts