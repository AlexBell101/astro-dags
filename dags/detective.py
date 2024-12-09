
from airflow import DAG
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
    dag_id="detective",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 9),
    catchup=False,
) as dag:

    # Task: Create Snowflake Table
    create_table_task = SnowflakeOperator(task_id='create_table', sql='''CREATE TABLE IF NOT EXISTS  (company_name STRING,
company_domain STRING,
funnel_stage STRING,
total_events INT,
total_events_mom FLOAT,
total_events_wow FLOAT,
unique_sources INT,
unique_sources_mom FLOAT,
unique_sources_wow FLOAT,
total_downloads INT,
total_views INT,
first_seen DATE,
last_seen DATE,
company_linkedin_url STRING,
company_industry STRING,
company_size STRING,
company_country STRING,
company_state STRING,
interest_start_date DATE,
investigation_start_date DATE,
experimentation_start_date DATE,
ongoing_usage_start_date DATE,
inactive_start_date DATE,
unique_total_downloads INT,
unique_total_views INT,
trend FLOAT)''', snowflake_conn_id='snowflake')


    # Task: Load Data into Snowflake
    load_to_snowflake = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        table="",
        stage="",
        file_format="(TYPE = CSV, FIELD_DELIMITER = ',', SKIP_HEADER = 1)",
        snowflake_conn_id="snowflake",
        s3_key="Scarf/company-rollups-scarf-export-2024-08-05-2024-09-05.csv",
        s3_bucket_name="scarfdata",
        aws_conn_id="s3",
    )

    # Task Dependencies
    create_table_task >> load_to_snowflake
    