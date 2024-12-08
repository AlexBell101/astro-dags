
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.models import Variable
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

# AWS and Snowflake secrets as Airflow Variables (adjust names as needed)
aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
snowflake_username = Variable.get("SNOWFLAKE_USERNAME")
snowflake_password = Variable.get("SNOWFLAKE_PASSWORD")

with DAG(
    dag_id="s3_snow",
    default_args=default_args,
    description="DAG to transfer data from S3 to Snowflake",
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 8),
    catchup=False,
) as dag:

    s3_to_snowflake = S3ToSnowflakeOperator(
        task_id="s3_to_snowflake",
        s3_bucket="scarfdata",
        s3_key="",
        aws_conn_id="aws_default",  # Ensure this connection exists in Airflow
        snowflake_conn_id="snowflake_default",  # Ensure this connection exists in Airflow
        stage="public.s3_snow_stage",
        file_format="(type=csv, field_delimiter=',', skip_header=1)",
    )





