
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="s3_snow",
    default_args=default_args,
    description="DAG to transfer data from S3 to Snowflake",
    schedule_interval="",
    start_date=datetime(2024, 12, 8),
    catchup=False,
) as dag:

    s3_to_snowflake = S3ToSnowflakeOperator(
        task_id="s3_to_snowflake",
        s3_bucket="scarfdata",
        s3_key="",
        aws_conn_id="aws_default",
        snowflake_conn_id=None,
        account="ia75458.rsggkwv.snowflakecomputing.com",
        warehouse="compute_wh",
        database="TEST_DB",
        schema="public",
        user="juliogeordio1",
        password="Newcastle1!",
        file_format="(type=csv, field_delimiter=',', skip_header=1)",
    )
    