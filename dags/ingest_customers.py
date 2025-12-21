from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow/src')

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_customers',
    default_args=default_args,
    description='Generate new customer signups',
    schedule_interval='0 3 * * *',  # Daily at 3 AM
    catchup=False,
    tags=['ingestion', 'customers', 'synthetic'],
)

def generate_customers(**context):
    """Generate 50-150 new customers"""
    print("=" * 70)
    print("CUSTOMER GENERATION")
    print("=" * 70)
    
    from ingestion.generate_customers import CustomerDataGenerator
    import random
    
    # Simulate daily signups (50-150 per day)
    num_new_customers = random.randint(50, 150)
    print(f"Generating {num_new_customers} new customers...")
    
    generator = CustomerDataGenerator(num_customers=num_new_customers)
    df = generator.run(load_to_snowflake=True, save_local=True)
    
    # Store metrics
    context['ti'].xcom_push(key='customers_count', value=len(df))
    context['ti'].xcom_push(key='execution_date', value=datetime.now().isoformat())
    
    print(f"✅ Generated {len(df)} customers")
    return len(df)

task_generate_customers = PythonOperator(
    task_id='generate_new_customers',
    python_callable=generate_customers,
    provide_context=True,
    dag=dag,
)