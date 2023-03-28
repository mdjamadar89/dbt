import yaml
import os, sys
try:
    from lib.helpers import aws_secrets
except:
    from helpers import aws_secrets


def create_profile(secrets, file_name='profiles.yml'):
    target = 'prod'
    main_target = {
        'prod': {
            'type': 'snowflake'
            ,'account': secrets['account']
            ,'user': secrets['username']
            ,'password': secrets['password']
            ,'role': os.getenv('ROLE', 'DEV_ROLE')
            ,'warehouse': secrets['warehouse']
            ,'database': os.getenv('DATABASE', 'DATA_WAREHOUSE_DEV')
            ,'schema': 'public'
            ,'threads': 4
            ,'client_session_keep_alive': False
            ,'target': target
        }
    }
    outputs = {'outputs': main_target, 'target': target}
    yaml_dict = {'dbt_etl': outputs}

    with open(file_name, 'w') as file:
        documents = yaml.dump(yaml_dict, file)

    return file_name


if __name__ == '__main__':
    if len(sys.argv) > 1:
        profile = sys.argv[1]
    else:
        profile = None

    credentials = aws_secrets.GetSecrets('snowflake_secrets', 'us-east-1', profile).secrets()
    create_profile(credentials)
