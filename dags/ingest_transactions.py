from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
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
    'ingest_transactions',
    default_args=default_args,
    description='Generate yesterday\'s transactions',
    schedule_interval='0 4 * * *',  # Daily at 4 AM
    catchup=False,
    tags=['ingestion', 'transactions', 'synthetic'],
)

def generate_transactions(**context):
    """Generate yesterday's transactions"""
    print("=" * 70)
    print("TRANSACTION GENERATION")
    print("=" * 70)
    
    from ingestion.generate_daily_transactions import DailyTransactionGenerator
    
    # Generate for yesterday
    yesterday = date.today() - timedelta(days=1)
    print(f"Generating transactions for {yesterday}...")
    
    generator = DailyTransactionGenerator(target_date=yesterday)
    df = generator.run(load_to_snowflake=True, save_local=True)
    
    # Store metrics
    context['ti'].xcom_push(key='transactions_count', value=len(df))
    context['ti'].xcom_push(key='total_revenue', value=float(df['total_amount'].sum()))
    context['ti'].xcom_push(key='execution_date', value=datetime.now().isoformat())
    
    print(f"✅ Generated {len(df)} transactions")
    print(f"   Revenue: ${df['total_amount'].sum():,.2f}")
    return len(df)

task_generate_transactions = PythonOperator(
    task_id='generate_daily_transactions',
    python_callable=generate_transactions,
    provide_context=True,
    dag=dag,
)