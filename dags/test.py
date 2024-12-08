from airflow import DAG
from datetime import datetime

# Test if the operator can be imported
try:
    from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
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
    dag_id="test_s3_to_snowflake",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Dummy task for now to ensure the DAG loads
    test_task = S3ToSnowflakeOperator(
        task_id="test_import_operator",
        s3_bucket="example-bucket",
        s3_key="example-key.csv",
        snowflake_conn_id="snowflake_default",
        stage="example_stage",
        file_format="(type=csv, field_delimiter=',')",
    )
