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
    dag_id="rabbit",
    default_args=default_args,
    schedule_interval="@hourly",  # Use Airflow's valid presets or a proper cron expression
    start_date=datetime(2024, 12, 9),
    catchup=False,
) as dag:

    # Task: Load Data into Snowflake
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="scarf",  # Replace with your Snowflake table name
        database="TEST_DB",  # Replace with your Snowflake database name
        schema="PUBLIC",  # Replace with your Snowflake schema name
        stage="SCARF2",  # Replace with your Snowflake stage name
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\\.csv",
        snowflake_conn_id="snowflake",  # Replace with the Astro Snowflake connection ID
    )
