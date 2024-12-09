
from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
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
    dag_id="s3tosnowflake",
    default_args=default_args,
    schedule_interval="Daily",
    start_date=datetime(2024, 12, 9),
    catchup=False,
) as dag:

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_file_to_s3",
        filename="/path/to/your/local/file.csv",
        dest_key="your-s3-prefix/",  # Replace with your S3 key
        dest_bucket_name="your-s3-bucket-name",  # Replace with your S3 bucket
        aws_conn_id="s3",  # Use the user-provided connection ID for AWS
    )

    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="your_table_name",  # Replace with your Snowflake table name
        stage="your_stage_name",  # Replace with your Snowflake stage
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        snowflake_conn_id="snowflake",  # Use the user-provided connection ID for Snowflake
    )

    upload_to_s3 >> load_to_snowflake
    