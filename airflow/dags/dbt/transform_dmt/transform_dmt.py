from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

CONNECTION_ID = "open_data_dwh_postgresql"
SCHEMA_NAME = "stg"

# The DAG below uses the BashOperator to activate the virtual environment and execute dbt_run for a dbt project.
PATH_TO_DBT_VENV = f"{os.environ['AIRFLOW_HOME']}/dbt-env/bin/activate"
# The path to the dbt project
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/transform_dmt/"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = f"/home/airflow/.local/bin/dbt"

profile_config = ProfileConfig(
    profile_name="transform_dmt",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

with DAG(
    dag_id='transform_dmt',
    default_args=default_args,
    description='Transform to data marts by transport from Moscow government',
    catchup=False,
    start_date=datetime(2024, 3, 17),
    schedule=None,
) as dag:
    init_source_data = BashOperator(
        task_id="init_source_data",
        bash_command=f"source $PATH_TO_DBT_VENV && {DBT_EXECUTABLE_PATH} run-operation init_source_data",
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=DBT_PROJECT_PATH,
    )
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        default_args={"retries": 2},
    )    
    
init_source_data >> transform_data
