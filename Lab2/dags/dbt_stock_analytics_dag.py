from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# dbt project configuration - paths inside Docker container
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_stock_analytics",
    default_args=default_args,
    description="Run dbt models for stock price analytics (ELT layer)",
    start_date=datetime(2025, 10, 1),
    schedule=None,  # Triggered by yf_stock_price_etl
    catchup=False,
    tags=["dbt", "ELT", "stock_analytics"],
) as dag:
    
    # Task 1: Install dbt dependencies (dbt_utils package)
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR}",
    )
    
    # Task 2: Run dbt models (staging -> transform -> analytics)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir {DBT_PROFILES_DIR}",
    )
    
    # Task 3: Run dbt tests (data quality checks)
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR}",
    )
    
    # Task 4: Run dbt snapshots (SCD Type 2)
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot --profiles-dir {DBT_PROFILES_DIR}",
    )
    
    # Task dependencies: deps -> run -> test -> snapshot
    dbt_deps >> dbt_run >> dbt_test >> dbt_snapshot
