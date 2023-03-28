from helpers import snowflake_connection, logger
import sys, time, os


def get_modifier_stmt(modifier):

    if modifier == 'increase':
        stmt = """
            ALTER WAREHOUSE prod_wh
            SET WAREHOUSE_SIZE = xsmall
                SCALING_POLICY = standard
                MIN_CLUSTER_COUNT = 1
                MAX_CLUSTER_COUNT = 4
                AUTO_SUSPEND = 300
                AUTO_RESUME = true;
        """
    elif modifier == 'decrease':
        stmt = """
            ALTER WAREHOUSE prod_wh
            SET WAREHOUSE_SIZE = xsmall
                SCALING_POLICY = economy
                MIN_CLUSTER_COUNT = 1
                MAX_CLUSTER_COUNT = 1
                AUTO_SUSPEND = 60
                AUTO_RESUME = true;
        """
    else:
        stmt = ""

    return stmt


def modify(modifier):
    try:
        # Make connection
        start_time = time.time()
        connection = snowflake_connection.SnowflakeConnection(database=os.getenv("DATA_LAKE")).connection()
        stmt = get_modifier_stmt(modifier)

        # Run SQL
        connection.cursor().execute(stmt)

        # Gather Logging info
        error_level = 5
        error_message = 'No errors'
        actions = 'No actions necessary'
        job_completion_status = 'Success'

    except Exception as e:
        connection.cursor().execute(f"ROLLBACK;")
        error_level = 3
        error_message = str(e)
        actions = 'Check the permissions and warehouse SQL syntax in modify_warehouse.py still works'
        job_completion_status = 'Failed'

    finally:
        connection.close()

    return logger.Logging('Modify warehouse size',
                            job_completion_status, error_level, error_message,
                            actions, start_time, os.path.basename(__file__)).logs()


if __name__ == '__main__':
    modifier = sys.argv[1]
    print(modify(modifier))