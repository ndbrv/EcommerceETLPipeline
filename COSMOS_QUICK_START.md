# 🚀 Astronomer Cosmos Quick Start

## What is Cosmos?

Cosmos automatically converts your **dbt models** into **Airflow tasks** with proper dependencies. Each model becomes a task!

```
dbt model (SQL file)  →  Airflow task (with dependencies)
```

## Setup (5 minutes)

### 1. Run Setup Script

**Windows (PowerShell):**
```powershell
.\setup_cosmos.ps1
```

**Mac/Linux:**
```bash
chmod +x setup_cosmos.sh
./setup_cosmos.sh
```

### 2. Configure Snowflake Connection

Go to Airflow UI: **http://localhost:8080** (default: `airflow`/`airflow`)

**Option A: Via UI** (Recommended)
1. Admin → Connections → "+" button
2. Fill in:
   - Connection Id: `snowflake_default`
   - Connection Type: `Snowflake`
   - Login: `<your_snowflake_user>`
   - Password: `<your_snowflake_password>`
   - Account: `uoc61155.us-east-1`
   - Warehouse: `COMPUTE_WH`
   - Database: `ECOMMERCE_DW`
   - Schema: `STAGING`
   - Role: `ACCOUNTADMIN`
3. Test → Save

**Option B: Via Setup DAG**
1. Edit `dags/setup_snowflake_connection.py` with your credentials
2. Run the DAG once
3. Disable it

### 3. Enable & Run DAG

1. Go to DAGs page in Airflow UI
2. Find **`dbt_ecommerce_simple`**
3. Toggle it ON (switch on the left)
4. Click ▶️ (play button) to trigger manually

## What You Get

### 📊 Automatic Task Graph

Your 23 dbt models become 23 Airflow tasks:

```
Staging Layer (5 tasks):
├─ stg_customers
├─ stg_orders
├─ stg_order_items  
├─ stg_products
└─ stg_transactions

Transformation Layer (5 tasks):
├─ transform_customers  ← depends on stg_customers
├─ transform_orders     ← depends on stg_orders, stg_transactions
├─ transform_order_items← depends on stg_order_items
├─ transform_products   ← depends on stg_products
└─ transform_transactions ← depends on stg_transactions

Curated Layer (13 tasks):
├─ dim_customers         ← depends on transform_customers
├─ dim_products          ← depends on transform_products
├─ dim_dates            ← standalone
├─ dim_geography        ← depends on transform_customers
├─ fact_orders          ← depends on transform_orders
├─ fact_order_line_items← depends on transform_order_items
├─ fact_daily_sales     ← depends on transform_orders
├─ metric_customer_cohorts ← depends on transform_customers
├─ metric_monthly_kpis    ← depends on transform_orders
├─ metric_payment_performance ← depends on transform_transactions
├─ metric_category_performance ← depends on transform_order_items
├─ metric_product_affinity ← depends on transform_order_items
└─ metric_product_performance ← depends on transform_order_items
```

### 🎯 Key Features

✅ **Automatic Dependencies**: Cosmos reads your `ref()` calls
✅ **Parallel Execution**: Independent models run simultaneously
✅ **Granular Retries**: Retry individual models, not the entire pipeline
✅ **Better Observability**: See status of each model
✅ **Incremental Builds**: Only rebuild what changed

## Daily Operations

### Run Manually
1. Go to DAG
2. Click ▶️ play button
3. Watch the graph view

### Schedule
Already set to run **daily at 3 AM**. Change in the DAG file:
```python
schedule_interval="0 3 * * *",  # 3 AM daily
```

### Run Specific Models
Edit `dags/dbt_cosmos_simple.py`:
```python
operator_args={
    "select": "staging",           # Only staging
    "select": "staging+",          # Staging + downstream
    "select": "dim_customers",     # Single model
    "select": "tag:daily",         # Tagged models
}
```

### Full Refresh
```python
operator_args={
    "full_refresh": True,  # Rebuild all incremental models
}
```

## Troubleshooting

### DAG Not Appearing
```bash
# Check for import errors
docker exec -it airflow_webserver python /opt/airflow/dags/dbt_cosmos_simple.py
```

### Connection Issues
```bash
# Test Snowflake connection
docker exec -it airflow_webserver airflow connections test snowflake_default
```

### dbt Errors
```bash
# Debug dbt configuration
docker exec -it airflow_webserver dbt debug --project-dir /opt/airflow/ecommerce_transform

# Run dbt manually
docker exec -it airflow_webserver dbt run --project-dir /opt/airflow/ecommerce_transform
```

### Check Logs
1. Go to DAG → Graph View
2. Click on a failed task
3. Click "Log" button
4. See detailed dbt output

## Files Created

| File | Purpose |
|------|---------|
| `dags/dbt_cosmos_simple.py` | Main DAG - runs all models |
| `dags/dbt_ecommerce_cosmos.py` | Advanced DAG with data generation |
| `dags/setup_snowflake_connection.py` | One-time connection setup |
| `dags/README_COSMOS_SETUP.md` | Detailed documentation |
| `setup_cosmos.sh` / `.ps1` | Automated setup scripts |
| `requirements.txt` | Python dependencies |
| `Dockerfile` | Updated with dbt + Cosmos |
| `docker-compose.yml` | Updated with dbt mount |

## Next Steps

### 1. Add Data Generation
Integrate your data generation scripts:
```python
from airflow.operators.python import PythonOperator

generate_data = PythonOperator(
    task_id="generate_data",
    python_callable=run_generation,
)

generate_data >> dbt_dag  # Run before dbt
```

### 2. Add Alerts
```python
default_args={
    "email_on_failure": True,
    "email": ["data-team@company.com"],
}
```

### 3. Monitor Performance
- Check task duration in Airflow UI
- Identify slow models
- Optimize dbt models accordingly

### 4. Add Tests
Cosmos automatically runs dbt tests!
```yaml
# In schema.yml
models:
  - name: dim_customers
    tests:
      - dbt_utils.recency:
          datepart: day
          field: last_updated_at
          interval: 1
```

### 5. Create dbt Docs
```bash
docker exec -it airflow_webserver dbt docs generate --project-dir /opt/airflow/ecommerce_transform
docker exec -it airflow_webserver dbt docs serve --project-dir /opt/airflow/ecommerce_transform --port 8001
```

Then go to: http://localhost:8001

## Architecture

```
┌──────────────────┐
│  Source Systems  │  (Your data generation scripts)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│   Snowflake RAW  │  (Raw tables: raw_customers, raw_orders, etc.)
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  Airflow DAG     │  (Cosmos orchestration)
│  ┌────────────┐  │
│  │ dbt Models │  │  Each model = 1 Airflow task
│  └────────────┘  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Snowflake Tables │
│  - STAGING       │  (Clean, typed data)
│  - TRANSFORM     │  (Business logic)
│  - CURATED       │  (BI-ready marts)
└──────────────────┘
```

## Benefits

| Before Cosmos | With Cosmos |
|--------------|-------------|
| 1 task for entire dbt run | 23 tasks (one per model) |
| Hard to debug failures | See exactly which model failed |
| Retry = rerun everything | Retry just the failed model |
| No visibility into progress | Real-time task status |
| Manual dependency management | Automatic from `ref()` |
| Sequential execution | Parallel where possible |

## Resources

- [Cosmos Documentation](https://astronomer.github.io/astronomer-cosmos/)
- [dbt Best Practices](https://docs.getdbt.com/best-practices)
- [Airflow Concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/)

## Support

If you encounter issues:
1. Check `dags/README_COSMOS_SETUP.md` for detailed troubleshooting
2. Review Airflow task logs
3. Run dbt commands manually to isolate issues
4. Check Docker container logs

---

**You're all set! 🎉**

Go to http://localhost:8080 and watch your dbt models run as beautiful Airflow tasks!

