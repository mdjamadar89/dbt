import unittest
import yaml
import os
import lib.dbt_profile as dbt


class DbtConnectionGeneration(unittest.TestCase):
    def test_profile_yml(self):
        os.environ["DATABASE"] = 'DATA_WAREHOUSE_TEST'
        secrets = {'account': 'test_account', 'username': 'test_user', 'password': 'fakeasinnotreal',
                   'database': 'DATA_WAREHOUSE_TEST', 'schema': 'public', 'warehouse': 'test', 'role': 'test'}
        dbt.create_profile(secrets)
        with open(r'./profiles.yml') as file:
            profile = yaml.load(file, Loader=yaml.FullLoader)
        values = profile['dbt_etl']['outputs']['prod']
        # get rid of default keys and change username key to user
        secrets['user'] = secrets.pop('username')
        default_keys = ['client_session_keep_alive', 'type', 'threads', 'target']
        for k in default_keys:
            values.pop(k)
        # test
        self.assertDictEqual(secrets, values)
        # clean up
        os.remove('./profiles.yml')


if __name__ == '__main__':
    unittest.main()
