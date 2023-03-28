import boto3
import json


class GetSecrets:
    def __init__(self, secret_name, region_name, profile=None):
        self.secret_name = secret_name
        self.region_name = region_name
        self.profile = profile

    def secrets(self):

        if self.profile is not None:
            session = boto3.session.Session(profile_name=self.profile)
        else:
            session = boto3.session.Session()

        client = session.client(
            service_name='secretsmanager',
            region_name=self.region_name
        )

        get_secret_value_response = client.get_secret_value(
            SecretId=self.secret_name
        )

        secrets = get_secret_value_response["SecretString"]
        return json.loads(secrets, strict=False)
