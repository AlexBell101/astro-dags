
from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
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
    dag_id="Aquafina",
    default_args=default_args,
    schedule_interval="Daily",
    start_date=datetime(2024, 12, 9),
    catchup=False,
) as dag:

    # Task 1: Create table in Snowflake (if checkbox selected)
    create_table_task = SnowflakeOperator(    task_id="create_table",    sql="
    CREATE TABLE IF NOT EXISTS  (
        id INT AUTOINCREMENT,
        data STRING,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ",    snowflake_conn_id="snowflake",)

    # Task 2: Upload local file to S3
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_file_to_s3",
        filename="s3://scarfdata/Scarf/",
        dest_key="Scarf/company-rollups-scarf-export-2024-08-05-2024-09-05.csv",
        dest_bucket_name="scarfdata",
        aws_conn_id="s3",
    )

    # Task 3: Load data from S3 into Snowflake
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="",
        stage="",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        snowflake_conn_id="snowflake",
    )

    # Define dependencies
    create_table_task >> upload_to_s3 >> load_to_snowflake
    