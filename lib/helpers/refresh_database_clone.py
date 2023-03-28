############################################################
# Update the database_clone_name and list_of_schemas variables in database_variables.py then run this script:
# python db_migrations/refresh_database_clone.py
# Leave your variables in database_variables.py as you are doing upgrades and downgrades as the script will use it as an environment variable
# In Prod and Stage, environment variables are already preset
############################################################


class RefreshDatabaseClone():
    def __init__(self, from_database_name, to_database_name, connection, grant_to_role='dev_role', grant_ownership=False, print=True):
        self.from_database_name = from_database_name
        self.to_database_name = to_database_name
        self.grant_to_role = grant_to_role
        self.connect = connection
        self.grant_ownership = grant_ownership
        self.print = print

    def call(self):
        if self.to_database_name != 'data_lake_dev':
            self.execute_refesh_stage(self.from_database_name, self.to_database_name, self.grant_to_role, self.grant_ownership)
        else:
            print("Please don't name your database data_lake_dev. Name it something more unique.")

    def execute_refesh_stage(self, from_database_name, to_database_name, grant_to_role, grant_ownership):
        self.execute_sql(f"CREATE OR REPLACE DATABASE {to_database_name} CLONE {from_database_name};")
        list_of_schemas = self.get_all_schemas(to_database_name)

        for role in [grant_to_role, 'ACCOUNTADMIN']:
            if grant_ownership:
                privilege = 'ALL PRIVILEGES' if role != grant_to_role else 'OWNERSHIP'
                suffix = 'WITH GRANT OPTION' if role != grant_to_role else 'COPY CURRENT GRANTS'
                if self.print:
                    print(f"DB: {to_database_name}, Role: {role}, grant_ownership: {grant_ownership}, privilege: {privilege}, suffix: {suffix}, in TRUE")
            else:
                privilege = 'ALL PRIVILEGES' if role == grant_to_role else 'OWNERSHIP'
                suffix = 'WITH GRANT OPTION' if role == grant_to_role else 'COPY CURRENT GRANTS'
                if self.print:
                    print(f"DB: {to_database_name}, Role: {role}, grant_ownership: {grant_ownership}, privilege: {privilege}, suffix: {suffix}, in ELSE")

            self.execute_sql(f"GRANT {privilege} ON DATABASE {to_database_name} TO ROLE {role} {suffix};")

            for schema in list_of_schemas:
                self.execute_sql(f"GRANT {privilege} ON SCHEMA {to_database_name}.{schema} TO {role} {suffix};")
                self.execute_sql(f"GRANT {privilege} ON ALL TABLES IN SCHEMA {to_database_name}.{schema} TO ROLE {role} {suffix};")
                self.execute_sql(f"GRANT {privilege} ON ALL VIEWS IN SCHEMA {to_database_name}.{schema} TO ROLE {role} {suffix};")
                self.execute_sql(f"GRANT {privilege} ON ALL SEQUENCES IN SCHEMA {to_database_name}.{schema} TO ROLE {role} {suffix};")
                self.execute_sql(f"GRANT {privilege} ON ALL STAGES IN SCHEMA {to_database_name}.{schema} TO ROLE {role} {suffix};")
                self.execute_sql(f"GRANT {privilege} ON ALL STREAMS IN SCHEMA {to_database_name}.{schema} TO ROLE {role} {suffix};")
                self.execute_sql(f"GRANT {privilege} ON ALL FILE FORMATS IN SCHEMA {to_database_name}.{schema} TO ROLE {role} {suffix};")
                self.execute_sql(f"GRANT {privilege} ON ALL PROCEDURES IN SCHEMA {to_database_name}.{schema} TO ROLE {role} {suffix};")
                self.execute_sql(f"GRANT {privilege} ON ALL TASKS IN SCHEMA {to_database_name}.{schema} TO ROLE {role} {suffix};")

    def execute_sql(self, stmt):
        # print(stmt)
        return self.connect.cursor().execute(stmt)

    def get_all_schemas(self, database_name):
        cur = self.connect.cursor()
        schemas = cur.execute(f"SELECT * FROM {database_name}.information_schema.schemata;")
        schema_list = []
        for schema in schemas:
            if schema[1].lower() != 'information_schema':
                schema_list.append(schema[1])
        cur.close()
        return schema_list