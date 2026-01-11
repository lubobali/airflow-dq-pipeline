from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from include.eczachly.poke_glue_partition import poke_glue_partition
from include.eczachly.scripts.polygon_stock_prices import fetch_stock_prices

# Get AWS credentials from Airflow Variables
aws_access_key_id = Variable.get("DATAEXPERT_AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("DATAEXPERT_AWS_SECRET_ACCESS_KEY")
aws_region = Variable.get("AWS_GLUE_REGION")
s3_bucket = Variable.get("AWS_S3_BUCKET_TABULAR")

def run_stock_prices_script(output_table: str, **context):
    """Execute the stock prices fetching function with AWS credentials"""
    run_date = context['ds']
    tickers_table = "zachwilson.stock_tickers"
    
    result = fetch_stock_prices(
        run_date=run_date,
        tickers_table=tickers_table,
        output_table=output_table,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=aws_region,
        s3_bucket=s3_bucket
    )

    return result

def run_dq_checks(staging_table: str, **context):
    """
    Data Quality checks on staging table before publishing to production.
    Big Tech pattern: Validate BEFORE it hits production!
    """
    from pyiceberg.catalog import load_catalog
    import boto3
    import os
    
    # Set up AWS
    if aws_access_key_id:
        os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
    if aws_secret_access_key:
        os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
    if aws_region:
        os.environ['AWS_REGION'] = aws_region
    
    boto3.setup_default_session(region_name=aws_region or 'us-west-2')
    
    catalog = load_catalog(
        name="glue_catalog",
        **{
            "type": "glue",
            "region": aws_region or 'us-west-2',
            "warehouse": f"s3://{s3_bucket}/iceberg-warehouse/",
        }
    )
    
    # Load staging table
    table = catalog.load_table(staging_table)
    df = table.scan().to_pandas()
    
    # DQ CHECK 1: Row count > 0
    row_count = len(df)
    if row_count == 0:
        raise ValueError(f"DQ FAILED: No rows in staging table {staging_table}")
    print(f"✅ DQ Check 1 PASSED: {row_count} rows found")
    
    # DQ CHECK 2: No NULL close prices
    null_prices = df['close'].isna().sum()
    if null_prices > 0:
        raise ValueError(f"DQ FAILED: {null_prices} NULL close prices in {staging_table}")
    print(f"✅ DQ Check 2 PASSED: No NULL close prices")
    
    # DQ CHECK 3: All prices > 0
    invalid_prices = (df['close'] <= 0).sum()
    if invalid_prices > 0:
        raise ValueError(f"DQ FAILED: {invalid_prices} invalid prices <= 0 in {staging_table}")
    print(f"✅ DQ Check 3 PASSED: All prices > 0")
    
    # DQ CHECK 4: Volume >= 0
    invalid_volume = (df['volume'] < 0).sum()
    if invalid_volume > 0:
        raise ValueError(f"DQ FAILED: {invalid_volume} negative volumes in {staging_table}")
    print(f"✅ DQ Check 4 PASSED: All volumes >= 0")
    
    print(f"✅ ALL DQ CHECKS PASSED for {staging_table}")
    return f"DQ PASSED: {row_count} rows validated"

def exchange_to_production(staging_table: str, production_table: str, **context):
    """
    Exchange data from staging to production.
    Big Tech pattern: Only publish AFTER DQ checks pass!
    """
    from pyiceberg.catalog import load_catalog
    import boto3
    import os
    
    # Set up AWS
    if aws_access_key_id:
        os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
    if aws_secret_access_key:
        os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
    if aws_region:
        os.environ['AWS_REGION'] = aws_region
    
    boto3.setup_default_session(region_name=aws_region or 'us-west-2')
    
    catalog = load_catalog(
        name="glue_catalog",
        **{
            "type": "glue",
            "region": aws_region or 'us-west-2',
            "warehouse": f"s3://{s3_bucket}/iceberg-warehouse/",
        }
    )
    
    # Load staging table data as PyArrow
    staging_tbl = catalog.load_table(staging_table)
    pa_data = staging_tbl.scan().to_arrow()
    
    # Load or create production table
    try:
        prod_tbl = catalog.load_table(production_table)
    except Exception:
        # Create production table with same schema as staging
        prod_tbl = catalog.create_table(
            identifier=production_table,
            schema=staging_tbl.schema(),
            partition_spec=staging_tbl.spec()
        )
    
    # PARTITION-SCOPED OVERWRITE: Staging table contains only run_date's data,
    # so overwrite replaces only that date's partition, preserving historical data.
    # Iceberg's DayTransform partitioning ensures partition-level atomicity.
    prod_tbl.overwrite(pa_data)
    
    row_count = len(pa_data)
    print(f"✅ EXCHANGED {row_count} rows from {staging_table} to {production_table} for date {context['ds']}")
    return f"Exchanged {row_count} rows to production for {context['ds']}"

def cleanup_staging(staging_table: str, **context):
    """
    Drop staging table after successful exchange.
    Big Tech pattern: Don't leave orphan tables behind!
    """
    from pyiceberg.catalog import load_catalog
    import boto3
    import os
    
    # Set up AWS
    if aws_access_key_id:
        os.environ['AWS_ACCESS_KEY_ID'] = aws_access_key_id
    if aws_secret_access_key:
        os.environ['AWS_SECRET_ACCESS_KEY'] = aws_secret_access_key
    if aws_region:
        os.environ['AWS_REGION'] = aws_region
    
    boto3.setup_default_session(region_name=aws_region or 'us-west-2')
    
    catalog = load_catalog(
        name="glue_catalog",
        **{
            "type": "glue",
            "region": aws_region or 'us-west-2',
            "warehouse": f"s3://{s3_bucket}/iceberg-warehouse/",
        }
    )
    
    # Drop staging table
    try:
        catalog.drop_table(staging_table)
        print(f"✅ CLEANUP: Dropped staging table {staging_table}")
    except Exception as e:
        print(f"⚠️ CLEANUP: Could not drop {staging_table}: {e}")
    
    return f"Cleanup complete for {staging_table}"

@dag(
    description="A DAG that waits for Polygon ticker data then fetches stock prices and creates Snowflake Iceberg table",
    default_args={
        "owner": "Zach Wilson",
        "start_date": datetime(2026, 1, 1),
        "retries": 1,
        "execution_timeout": timedelta(hours=2),
    },
    start_date=datetime(2026, 1, 1),
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    tags=["community", "polygon", "iceberg", "stock-prices", "snowflake"],
)
def massive_stock_prices_dag():
    upstream_table = 'zachwilson.stock_tickers'
    
    wait_for_tickers = PythonOperator(
        task_id='wait_for_polygon_tickers',
        python_callable=poke_glue_partition,
        op_kwargs={
            "table": upstream_table,
            "partition": 'date_day={{ ds }}',
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_region": aws_region
        },
        provide_context=True
    )
    
    # Staging table: production name + _stg_ + date (Zach's convention)
    production_table = "zachwilson.stock_prices"
    staging_table = "zachwilson.stock_prices_stg_{{ ds_nodash }}"
    
    fetch_to_staging = PythonOperator(
        task_id='fetch_to_staging',
        python_callable=run_stock_prices_script,
        op_kwargs={
            "output_table": staging_table
        },
        provide_context=True
    )
    
    run_dq_checks_task = PythonOperator(
        task_id='run_dq_checks',
        python_callable=run_dq_checks,
        op_kwargs={
            "staging_table": staging_table
        },
        provide_context=True
    )
    
    exchange_task = PythonOperator(
        task_id='exchange_to_production',
        python_callable=exchange_to_production,
        op_kwargs={
            "staging_table": staging_table,
            "production_table": production_table
        },
        provide_context=True
    )
    
    cleanup_task = PythonOperator(
        task_id='cleanup_staging',
        python_callable=cleanup_staging,
        op_kwargs={
            "staging_table": staging_table
        },
        provide_context=True
    )

    wait_for_tickers >> fetch_to_staging >> run_dq_checks_task >> exchange_task >> cleanup_task

massive_stock_prices_dag()
