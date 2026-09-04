# Astronomer Cosmos Setup for DBT + Airflow

## Overview
This setup integrates dbt with Airflow using Astronomer Cosmos, which automatically converts each dbt model into an Airflow task with proper dependency resolution.

## Setup Instructions

### 1. Rebuild Docker Containers
The Dockerfile has been updated to include the necessary packages:
- `dbt-snowflake==1.10.4`
- `astronomer-cosmos==1.7.0`
- `apache-airflow-providers-snowflake==5.6.1`

```bash
# Stop existing containers
docker-compose down

# Rebuild with new dependencies
docker-compose build

# Start containers
docker-compose up -d
```

### 2. Set Up Snowflake Connection

**Option A: Using the Setup DAG (Recommended)**

1. Go to Airflow UI: http://localhost:8080
2. Find the DAG `setup_snowflake_connection`
3. Edit `dags/setup_snowflake_connection.py` with your Snowflake credentials:
   ```python
   login="YOUR_SNOWFLAKE_USER",
   password="YOUR_SNOWFLAKE_PASSWORD",
   ```
4. Trigger the DAG manually
5. Disable the DAG after successful run

**Option B: Manual Setup via Airflow UI**

1. Go to Admin → Connections
2. Click the "+" button
3. Fill in:
   - **Connection Id**: `snowflake_default`
   - **Connection Type**: `Snowflake`
   - **Login**: Your Snowflake username
   - **Password**: Your Snowflake password
   - **Account**: `uoc61155.us-east-1`
   - **Warehouse**: `COMPUTE_WH` (or your warehouse)
   - **Database**: `ECOMMERCE_DW`
   - **Role**: `ACCOUNTADMIN` (or your role)
   - **Schema**: `STAGING`
4. Click "Test" then "Save"

**Option C: Using Environment Variables**

Add to `docker-compose.yml` environment section:
```yaml
- AIRFLOW_CONN_SNOWFLAKE_DEFAULT=snowflake://USER:PASSWORD@uoc61155.us-east-1/ECOMMERCE_DW?warehouse=COMPUTE_WH&role=ACCOUNTADMIN
```

### 3. Verify DBT Project

Make sure your dbt profile is accessible:
```bash
# Check if dbt can find the project
docker exec -it airflow_webserver dbt debug --project-dir /opt/airflow/ecommerce_transform
```

### 4. Run the DAG

Two DAGs are available:

**DAG 1: `dbt_ecommerce_full_pipeline`**
- Runs the entire dbt project
- Each dbt model becomes a separate Airflow task
- Automatic dependency resolution based on dbt lineage
- Best for: Production runs

**DAG 2: `dbt_ecommerce_with_ingestion`**
- Includes data generation before dbt
- Runs dbt in layers: staging → transformation → curated
- Best for: Development and testing

### 5. Monitor Execution

In the Airflow UI, you'll see:
- **Task Graph**: One task per dbt model
- **Dependencies**: Automatically resolved from dbt DAG
- **Logs**: Detailed dbt logs for each model
- **Task Duration**: Per-model execution time

## DAG Features

### Automatic Task Generation
Cosmos creates individual Airflow tasks for:
- Each dbt model
- Each dbt test
- Each dbt snapshot
- Each dbt seed

### Smart Execution
- Only runs models that changed (incremental builds)
- Parallel execution where possible
- Proper retry logic
- Detailed logging per model

### DAG Configuration

#### Change Schedule
Edit `schedule_interval` in `dags/dbt_ecommerce_cosmos.py`:
```python
schedule_interval="0 3 * * *",  # Daily at 3 AM
# Or:
schedule_interval="0 */6 * * *",  # Every 6 hours
schedule_interval=None,  # Manual only
```

#### Change Target Environment
Update `target_name` in ProfileConfig:
```python
target_name="prod",  # or "dev", "staging"
```

#### Run Specific Models
Add `select` to operator_args:
```python
operator_args={
    "select": "staging+",  # Staging and downstream
    "select": "dim_customers",  # Single model
    "select": "tag:daily",  # Models with 'daily' tag
}
```

#### Full Refresh
```python
operator_args={
    "full_refresh": True,
}
```

## Troubleshooting

### DAG Not Appearing
```bash
# Check Airflow can import the DAG
docker exec -it airflow_webserver python /opt/airflow/dags/dbt_ecommerce_cosmos.py
```

### dbt Command Not Found
```bash
# Verify dbt is installed
docker exec -it airflow_webserver which dbt
docker exec -it airflow_webserver dbt --version
```

### Connection Issues
```bash
# Test Snowflake connection from Airflow
docker exec -it airflow_webserver airflow connections test snowflake_default
```

### Check dbt Profile
```bash
# Debug dbt configuration
docker exec -it airflow_webserver dbt debug --project-dir /opt/airflow/ecommerce_transform
```

## Benefits of Cosmos

✅ **Automatic Task Generation**: No manual task creation
✅ **Dependency Resolution**: Uses dbt's ref() graph
✅ **Better Observability**: See each model's status
✅ **Granular Retries**: Retry individual models
✅ **Parallel Execution**: Run independent models simultaneously
✅ **Incremental Builds**: Only run what changed
✅ **dbt Docs Integration**: Can serve dbt docs from Airflow

## Next Steps

1. **Add Alerts**: Configure email/Slack notifications
2. **Add SLAs**: Set expectations for task duration
3. **Create Views**: Expose DAG runs in BI tools
4. **Add dbt Tests**: Integrate data quality checks
5. **Enable dbt Docs**: Serve documentation via Airflow

## References

- [Astronomer Cosmos Docs](https://astronomer.github.io/astronomer-cosmos/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Airflow Documentation](https://airflow.apache.org/docs/)

