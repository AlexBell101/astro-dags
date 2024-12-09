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
    dag_id="booty",
    default_args=default_args,
    schedule_interval="Hourly",
    start_date=datetime(2024, 12, 9),
    catchup=False,
) as dag:

    # Task: Load Data into Snowflake
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="your_table_name",  # Replace with your Snowflake table name
        database="your_database_name",  # Replace with your Snowflake database name
        schema="your_schema_name",  # Replace with your Snowflake schema name
        stage="your_stage_name",  # Replace with your Snowflake stage name
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\\.csv",
        snowflake_conn_id="snowflake",  # Replace with your Snowflake connection ID
        aws_conn_id="s3",  # Replace with your AWS connection ID
    )
