
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
    dag_id="walling",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2024, 12, 9),
    catchup=False,
) as dag:

    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="scarf",
        database="TEST_DB",
        schema="public",
        snowflake_conn_id="snowflake",
        stage="scarf2",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\.csv",
        s3_key="Scarf/company-rollups-scarf-export-2024-08-05-2024-09-05.csv",
        aws_conn_id="s3",
    )
    