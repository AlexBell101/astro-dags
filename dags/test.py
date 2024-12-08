try:
    from airflow.providers.amazon.aws.transfers.s3_to_snowflake import S3ToSnowflakeOperator
    print("Import successful: transfers.s3_to_snowflake")
except ImportError:
    print("Failed: transfers.s3_to_snowflake")

try:
    from airflow.providers.amazon.aws.operators.s3_to_snowflake import S3ToSnowflakeOperator
    print("Import successful: operators.s3_to_snowflake")
except ImportError:
    print("Failed: operators.s3_to_snowflake")
