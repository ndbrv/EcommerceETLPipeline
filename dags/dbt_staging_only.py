"""
dbt Staging Layer Only - Just runs staging models
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

# Staging layer only
dbt_staging_only = DbtDag(
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
                "schema": "staging",
            },
        ),
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH,
    ),
    dag_id="dbt_staging_only",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
    default_args=default_args,
    description="dbt staging layer only",
    tags=["dbt", "ecommerce", "staging"],
    operator_args={
        "select": "staging",  # Only staging models
        "install_deps": True,
    },
)
