"""
Simple dbt Ecommerce Full Pipeline using Astronomer Cosmos
Runs all dbt models in sequence: staging -> transformation -> curated
"""

from datetime import datetime, timedelta
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# Paths
DBT_PROJECT_PATH = Path("/opt/airflow/ecommerce_transform")
DBT_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt"

# Default arguments
default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create the dbt DAG that runs all models
dbt_ecommerce_full_pipeline = DbtDag(
    # Project config
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    
    # Profile config - uses Airflow Snowflake connection (NO profiles.yml needed!)
    profile_config=ProfileConfig(
        profile_name="ecommerce_transform",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_default",  # Set this in Airflow UI: Admin → Connections
            profile_args={
                "database": "ECOMMERCE_DW",
                "schema": "staging",  # Default schema (will be overridden by dbt models)
            },
        ),
    ),
    
    # Execution config
    execution_config=ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    ),
    
    # Airflow DAG parameters
    dag_id="dbt_ecommerce_full_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 3 * * *",  # 3 AM daily
    catchup=False,
    default_args=default_args,
    description="Full ecommerce dbt pipeline - runs all models (staging, transformation, curated)",
    tags=["dbt", "ecommerce", "cosmos", "full_pipeline"],
    
    # Operator args - passed to all dbt operators
    operator_args={
        "install_deps": True,  # Run dbt deps before running
        "full_refresh": False,
    },
)
