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
    'ingest_products',
    default_args=default_args,
    description='Ingest products from DummyJSON API',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    tags=['ingestion', 'products', 'api'],
)

def ingest_products(**context):
    """Fetch products from DummyJSON API"""
    print("=" * 70)
    print("PRODUCT INGESTION")
    print("=" * 70)
    
    from ingestion.product_ingestion import DummyJSONIngestion
    
    ingestion = DummyJSONIngestion()
    df = ingestion.run(load_to_snowflake=True, save_local=False)
    
    # Store metrics
    context['ti'].xcom_push(key='products_count', value=len(df))
    context['ti'].xcom_push(key='execution_date', value=datetime.now().isoformat())
    
    print(f"✅ Ingested {len(df)} products")
    return len(df)

task_ingest_products = PythonOperator(
    task_id='fetch_products',
    python_callable=ingest_products,
    provide_context=True,
    dag=dag,
)