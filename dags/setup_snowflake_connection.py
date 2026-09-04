"""
One-time DAG to set up Snowflake connection in Airflow
Run this once, then disable it.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from airflow import settings
import json


def create_snowflake_connection():
    """Create Snowflake connection in Airflow"""
    
    # Connection details - update these with your actual credentials
    conn_id = "snowflake_default"
    
    # Check if connection already exists
    session = settings.Session()
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if existing_conn:
        print(f"Connection {conn_id} already exists. Updating...")
        session.delete(existing_conn)
        session.commit()
    
    # Create new connection
    new_conn = Connection(
        conn_id=conn_id,
        conn_type="snowflake",
        login="YOUR_SNOWFLAKE_USER",  # Update this
        password="YOUR_SNOWFLAKE_PASSWORD",  # Update this
        schema="STAGING",
        extra=json.dumps({
            "account": "uoc61155.us-east-1",  # From your .env
            "warehouse": "COMPUTE_WH",  # Update if different
            "database": "ECOMMERCE_DW",
            "role": "ACCOUNTADMIN",  # Update if different
            "region": "us-east-1",
        })
    )
    
    session.add(new_conn)
    session.commit()
    session.close()
    
    print(f"✅ Snowflake connection '{conn_id}' created successfully!")
    print("You can now use it in your DAGs.")
    print("\n⚠️  Remember to disable this DAG after running it once!")


with DAG(
    dag_id="setup_snowflake_connection",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["setup", "one-time"],
    description="One-time setup: Create Snowflake connection in Airflow",
) as dag:
    
    setup_conn = PythonOperator(
        task_id="create_snowflake_connection",
        python_callable=create_snowflake_connection,
    )

