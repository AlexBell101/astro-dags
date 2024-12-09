
from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    dag_id="Aqua",
    default_args=default_args,
    schedule_interval="Daily",
    start_date=datetime(2024, 12, 9),
    catchup=False,
) as dag:

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_file_to_s3",
        filename="/path/to/your/local/file.csv",
        dest_key="",
        dest_bucket_name="scarfdata",
        aws_conn_id="s3",
    )

    
    create_table = SnowflakeOperator(
        task_id="create_table",
        sql="""CREATE TABLE my_table (column1 STRING, column2 INT);""",
        snowflake_conn_id="snowflake",
    )
    

    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="",
        stage="",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        snowflake_conn_id="snowflake",
    )

    upload_to_s3 >> load_to_snowflake
    create_table >> load_to_snowflake
    