
from airflow import DAG
from datetime import datetime

try:
    from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
except ImportError:
    try:
        from airflow.providers.amazon.aws.operators.s3_to_snowflake import S3ToSnowflakeOperator
    except ImportError as e:
        raise ImportError(f"Failed to import S3ToSnowflakeOperator: {e}")

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
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 8),
    catchup=False,
) as dag:
    s3_to_snowflake = S3ToSnowflakeOperator(
        task_id="s3_to_snowflake",
        s3_bucket="scarfdata",
        s3_key="",
        aws_conn_id="aws_default",
        snowflake_conn_id="snowflake_default",
        stage="public.s3_snow_stage",
        file_format="(type=csv, field_delimiter=',', skip_header=1)",
    )
    