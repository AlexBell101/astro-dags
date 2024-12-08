
from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from datetime import datetime

with DAG(
    dag_id="s3_snow",
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 8),
    catchup=False,
) as dag:

    s3_to_snowflake = S3ToSnowflakeOperator(
        task_id="s3_to_snowflake",
        s3_bucket="scarfdata",
        s3_key="",
        snowflake_conn_id="snowflake_default",
        stage="public.s3_snow_stage",
        file_format="(type=csv)",  # Adjust as needed
    )

    s3_to_snowflake
    