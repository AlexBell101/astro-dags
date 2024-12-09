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
    dag_id="bellus",
    default_args=default_args,
    schedule_interval="Hourly",  # Adjusted based on your requirement
    start_date=datetime(2024, 12, 9),
    catchup=False,
) as dag:
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="scarf",  # Replace with your Snowflake table name
        stage="s3_stage",  # Replace with your Snowflake stage
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        pattern=".*\.csv",
        snowflake_conn_id="snowflake",  # Replace with your Snowflake connection ID
        s3_bucket_name="scarfdata",  # Replace with your S3 bucket name
        s3_key="Scarf/company-rollups-scarf-export-2024-08-05-2024-09-05.csv",  # Replace with your S3 key
    )
