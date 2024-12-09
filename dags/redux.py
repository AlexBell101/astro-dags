
from airflow import DAG
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="redux",
    default_args=default_args,
    schedule_interval="Daily",
    start_date=datetime(2024, 12, 9),
    catchup=False,
) as dag:

    # Task: Load Data into Snowflake
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="scarf",
        database="TEST_DB",
        schema="public",
        stage="",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\.csv",
        snowflake_conn_id="snowflake",
        aws_conn_id="s3",
    )
    