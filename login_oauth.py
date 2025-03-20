import logging
import os
import sys
import time
from snowflake.snowpark import Session
from snowflake.snowpark.exceptions import *

# conn = snowflake.connector.connect(
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
# )



def get_logger():
    """
    Get a logger for local logging.
    """
    logger = logging.getLogger("job-tutorial")
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_login_token():
  with open('/snowflake/session/token', 'r') as f:
    return f.read()

def get_connection_params():
    """
    Construct Snowflake connection params from environment variables.
    """
    if os.path.exists("/snowflake/session/token"):
        return {
            "account": SNOWFLAKE_ACCOUNT,
            "host": SNOWFLAKE_HOST,
            "authenticator": "oauth",
            "token": get_login_token(),
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }
    else:
        return print('Token não encontrado')


# Execute a SQL query
def run_job():
    """
    Main body of this job.
    """
    logger = get_logger()
    logger.info("Job started")

    # Parse input arguments
    # args = get_arg_parser().parse_args()
    # query = args.query
    # result_table = args.result_table

    # Start a Snowflake session, run the query and write results to specified table
    with Session.builder.configs(get_connection_params()).create() as session:
        # Print out current session context information.
        database = session.get_current_database()
        schema = session.get_current_schema()
        warehouse = session.get_current_warehouse()
        role = session.get_current_role()
        logger.info(
            f"Connection succeeded. Current session context: database={database}, schema={schema}, warehouse={warehouse}, role={role}"
        )

        # Execute query and persist results in a table.
        # logger.info(
        #     f"Executing query [{query}] and writing result to table [{result_table}]"
        # )
        session.sql('ALTER GIT REPOSITORY airflow_dags_repo FETCH').collect()
        session.sql('REMOVE @airflow_db.airflow_schema.airflow_dags').collect()
        session.sql('COPY FILES INTO @airflow_db.airflow_schema.airflow_dags FROM @airflow_dags_repo/branches/main/').collect()

        session.sql('REMOVE @airflow_db.airflow_schema.airflow_dbt').collect()
        session.sql('COPY FILES INTO @airflow_db.airflow_schema.airflow_dbt FROM @airflow_dags_repo/branches/main/dbt/').collect()

        # If the table already exists, the query result must match the table scheme.
        # If the table does not exist, this will create a new table.
        # res.write.mode("append").save_as_table(result_table)

    logger.info("Job finished")


if __name__ == "__main__":
    while True:
        run_job()
        time.sleep(30)
    