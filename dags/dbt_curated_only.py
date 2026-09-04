"""
dbt Curated Layer Only - Just runs curated/mart models
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

# Curated layer only
dbt_curated_only = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=ProfileConfig(
        profile_name="ecommerce_transform",
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id="snowflake_default",
            profile_args={
                "database": "ECOMMERCE_DW",
                "schema": "curated",
            },
        ),
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    ),
    dag_id="dbt_curated_only",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
    default_args=default_args,
    description="dbt curated layer only",
    tags=["dbt", "ecommerce", "curated"],
    operator_args={
        "select": "curated",  # Only curated models
        "install_deps": False,  # Deps should already be installed
    },
)
