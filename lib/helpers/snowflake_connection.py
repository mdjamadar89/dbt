import snowflake.connector
try:
    import aws_secrets
except:
    from helpers import aws_secrets
import os


class SnowflakeConnection():
    def __init__(self, secret_name='snowflake-secrets', region_name='us-west-2', database = None, schema = None, role = None, warehouse = None, autocommit=True):
        self.database = database
        self.schema = schema
        self.role = role
        self.warehouse = warehouse
        self.autocommit = autocommit
        secret_name = secret_name if os.getenv("SECRET_NAME") is None else os.getenv("SECRET_NAME")

        self.credentials = aws_secrets.GetSecrets(secret_name, region_name).secrets()
        self.snowflake_credentials = self.snowflake_credentials()

    def snowflake_credentials(self):
        url = {
            "account": self.credentials["account"],
            "user": self.credentials["username"],
            "password": self.credentials["password"],
            "database": self.database or self.credentials["database"],
            "schema": self.schema or self.credentials["schema"],
            "warehouse": self.warehouse or self.credentials["warehouse"],
            "role": self.role or self.credentials["role"],
            "numpy": True
        }
        return url

    def connection(self) :
        con = snowflake.connector.connect(
            account = self.snowflake_credentials['account'],
            user = self.snowflake_credentials['user'],
            password = self.snowflake_credentials['password'],
            database = self.snowflake_credentials['database'],
            schema = self.snowflake_credentials['schema'],
            warehouse = self.snowflake_credentials['warehouse'],
            role = self.snowflake_credentials['role'],
            autocommit = self.autocommit
        )
        return con