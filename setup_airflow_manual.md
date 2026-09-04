# Manual Airflow Connection Setup

If the automated script doesn't work, follow these manual steps:

## Step 1: Access Airflow UI

1. Make sure Airflow is running:
   ```powershell
   docker compose up -d
   ```

2. Wait 60 seconds for initialization

3. Go to **http://localhost:8080**

4. Login:
   - Username: `airflow`
   - Password: `airflow`

## Step 2: Create Snowflake Connection

1. In Airflow UI, click **Admin** → **Connections**

2. Click the **+** button (Add a new record)

3. Fill in the following fields:

   | Field | Value |
   |-------|-------|
   | **Connection Id** | `snowflake_default` |
   | **Connection Type** | `Snowflake` |
   | **Host** | `uoc61155.us-east-1` |
   | **Schema** | `staging` |
   | **Login** | `NDBRV3` |
   | **Password** | _(your Snowflake password — see `SNOWFLAKE_PASSWORD` in `.env`)_ |
   | **Port** | (leave empty) |

4. In the **Extra** field, paste this JSON:

   ```json
   {
     "account": "uoc61155.us-east-1",
     "warehouse": "COMPUTE_WH",
     "database": "ECOMMERCE_DW",
     "role": "ACCOUNTADMIN"
   }
   ```

5. Click **Test** button (should show "Connection successfully tested")

6. Click **Save**

## Step 3: Verify Connection

### Option A: In Airflow UI

1. Go back to **Admin** → **Connections**
2. Find `snowflake_default` in the list
3. Click the edit icon to verify fields

### Option B: Test in Container

```powershell
docker exec airflow_webserver python -c "
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection('snowflake_default')
print(f'Connection found: {conn.conn_id}')
print(f'Host: {conn.host}')
print(f'Login: {conn.login}')
print(f'Schema: {conn.schema}')
"
```

## Step 4: Enable and Run DAG

1. In Airflow UI, go to **DAGs**

2. Find `dbt_ecommerce_simple` (or `dbt_ecommerce_full_pipeline`)

3. Toggle the switch to **enable** it

4. Click the **play** button to trigger a run

5. Watch the logs for dbt execution

## Troubleshooting

### "Connection not found"
- Make sure you saved the connection with ID exactly as `snowflake_default`
- Restart Airflow scheduler: `docker restart airflow_scheduler`

### "Test connection failed"
- Check your Snowflake credentials are correct
- Make sure your Snowflake trial hasn't expired
- Verify network connectivity to Snowflake

### "profiles.yml not found" error
- This is expected! Cosmos doesn't need profiles.yml
- The DAGs use Airflow connections instead
- If you need to run manual dbt commands, use the profiles.yml in `ecommerce_transform/` folder

### Manual dbt Test

If you want to test dbt manually:

```powershell
docker exec airflow_webserver dbt debug --project-dir /opt/airflow/ecommerce_transform --profiles-dir /opt/airflow/ecommerce_transform
```

This uses the `ecommerce_transform/profiles.yml` file with environment variables.
