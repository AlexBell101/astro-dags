
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
    dag_id="ventura",
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 9),
    catchup=False,
) as dag:

    # Task: Create Snowflake Table
    create_table_task = SnowflakeOperator(
        task_id='create_table',
        sql='CREATE TABLE IF NOT EXISTS {snowflake_table} ({table_columns})',
        snowflake_conn_id='{snowflake_connection_id}',
    )

    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="",
        stage="",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\.csv",
        snowflake_conn_id="snowflake",
        s3_key="Scarf/company-rollups-scarf-export-2024-08-05-2024-09-05.csv",
        s3_bucket_name="scarfdata",
        aws_conn_id="s3",
    )

    create_table_task >> load_to_snowflake
    