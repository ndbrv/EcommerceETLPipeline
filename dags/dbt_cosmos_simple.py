"""
Simplified DBT Ecommerce Pipeline using Astronomer Cosmos

This is a simpler version that runs all dbt models in one go.
Perfect for getting started!
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Configuration
DBT_PROJECT_PATH = Path("/opt/airflow/ecommerce_transform")
DBT_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt"

# Create the Cosmos DAG - This automatically creates tasks for all dbt models!
dbt_dag = DbtDag(
    # ===== Project Configuration =====
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    
    # ===== Snowflake Connection =====
    # Uses the "snowflake_default" connection you set up in Airflow
    profile_config=ProfileConfig(
        profile_name="ecommerce_transform",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_default",
            profile_args={
                "database": "ECOMMERCE_DW",
                "schema": "STAGING",
            },
        ),
    ),
    
    # ===== Execution Configuration =====
    execution_config=ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    ),
    
    # ===== Standard Airflow DAG Parameters =====
    dag_id="dbt_ecommerce_simple",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 3 * * *",  # Run daily at 3 AM
    catchup=False,
    
    # Default args for all tasks
    default_args={
        "owner": "data_team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    
    # DAG metadata
    description="Ecommerce dbt pipeline - automatically creates tasks for each model",
    tags=["dbt", "ecommerce", "cosmos"],
    
    # ===== DBT-Specific Arguments =====
    operator_args={
        "install_deps": True,  # Run "dbt deps" before running models
        "full_refresh": False,  # Set to True to rebuild all incremental models
    },
)

"""
What does this DAG do?

1. Cosmos automatically:
   - Scans your dbt project
   - Creates one Airflow task per dbt model
   - Resolves dependencies from dbt's ref() calls
   - Runs models in the correct order

2. You get:
   - ✅ stg_customers task
   - ✅ stg_orders task
   - ✅ stg_products task
   - ✅ stg_transactions task
   - ✅ stg_order_items task
   - ✅ transform_customers task
   - ✅ transform_orders task
   - ✅ transform_products task
   - ✅ transform_transactions task
   - ✅ transform_order_items task
   - ✅ dim_customers task
   - ✅ dim_products task
   - ✅ dim_dates task
   - ✅ dim_geography task
   - ✅ fact_orders task
   - ✅ fact_order_line_items task
   - ✅ fact_daily_sales task
   - ✅ metric_customer_cohorts task
   - ✅ metric_monthly_kpis task
   - ✅ metric_payment_performance task
   - ✅ metric_category_performance task
   - ✅ metric_product_affinity task
   - ✅ metric_product_performance task
   
   All with automatic dependencies!

3. To customize:
   - Run specific models: operator_args={"select": "staging"}
   - Exclude models: operator_args={"exclude": "staging"}
   - Full refresh: operator_args={"full_refresh": True}
   - Change schedule: schedule_interval="0 */6 * * *"
"""

