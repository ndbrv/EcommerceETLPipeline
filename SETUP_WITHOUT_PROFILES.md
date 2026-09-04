# Running Cosmos DBT Without profiles.yml

**Good news!** With Astronomer Cosmos, you don't need `profiles.yml` at all. 🎉

## How It Works

Cosmos uses **Airflow Connections** to manage credentials, which means:
- ✅ No separate `profiles.yml` file needed
- ✅ Credentials managed in Airflow UI
- ✅ Same connection used across all DAGs
- ✅ Better security (credentials not in files)
- ✅ Easier to promote between environments (dev/staging/prod)

## Setup Steps

### 1. Start Airflow

```powershell
docker compose up -d
```

### 2. Create Snowflake Connection in Airflow

1. Go to **http://localhost:8080**
2. Login: `airflow` / `airflow`
3. Click **Admin → Connections**
4. Click the **+** button
5. Fill in:

```
Connection ID: snowflake_default
Connection Type: Snowflake
Host: uoc61155.us-east-1
Schema: staging
Login: NDBRV3
Password: <your_snowflake_password>
Extra: {
  "account": "uoc61155.us-east-1",
  "warehouse": "COMPUTE_WH",
  "database": "ECOMMERCE_DW",
  "role": "ACCOUNTADMIN"
}
```

6. Click **Test** (should see "Connection successfully tested")
7. Click **Save**

### 3. Run Your DAG

Your DAGs in `dags/dbt_ecommerce_cosmos.py` are already configured to use this connection!

```python
profile_mapping=SnowflakeUserPasswordProfileMapping(
    conn_id="snowflake_default",  # ← Uses Airflow Connection
    profile_args={
        "database": "ECOMMERCE_DW",
        "schema": "staging",
    },
)
```

## What About Manual dbt Commands?

If you need to run `dbt debug` or `dbt run` manually inside the container, you have two options:

### Option A: Use Environment Variables

```bash
docker exec airflow_webserver bash -c "
export DBT_SNOWFLAKE_ACCOUNT='uoc61155.us-east-1'
export DBT_SNOWFLAKE_USER='NDBRV3'
export DBT_SNOWFLAKE_PASSWORD=\"$SNOWFLAKE_PASSWORD\"
export DBT_SNOWFLAKE_ROLE='ACCOUNTADMIN'
export DBT_SNOWFLAKE_WAREHOUSE='COMPUTE_WH'
export DBT_SNOWFLAKE_DATABASE='ECOMMERCE_DW'

dbt debug --project-dir /opt/airflow/ecommerce_transform --profiles-dir /opt/airflow/ecommerce_transform
"
```

And update `ecommerce_transform/profiles.yml` to use env vars:

```yaml
ecommerce_transform:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('DBT_SNOWFLAKE_USER') }}"
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('DBT_SNOWFLAKE_ROLE') }}"
      warehouse: "{{ env_var('DBT_SNOWFLAKE_WAREHOUSE') }}"
      database: "{{ env_var('DBT_SNOWFLAKE_DATABASE') }}"
      schema: staging
      threads: 4
```

### Option B: Just Keep a Simple profiles.yml for Manual Use

Keep `ecommerce_transform/profiles.yml` with your credentials, but DON'T mount it to `/home/airflow/.dbt/`. 

It's only used when you manually run dbt commands like:

```bash
docker exec airflow_webserver dbt debug --project-dir /opt/airflow/ecommerce_transform --profiles-dir /opt/airflow/ecommerce_transform
```

## Environment Variable Approach (Most Secure)

### 1. Add to `.env` file

```env
# Snowflake credentials
SNOWFLAKE_ACCOUNT=uoc61155.us-east-1
SNOWFLAKE_USER=NDBRV3
SNOWFLAKE_PASSWORD=<your_snowflake_password>
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ECOMMERCE_DW
```

### 2. Update docker-compose.yml

Already done! See the environment section.

### 3. Update profiles.yml to use env vars

```yaml
ecommerce_transform:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE') }}"
      schema: staging
      threads: 4
```

### 4. Create Airflow Connection via Python

```python
# In your DAG or a one-time setup script
from airflow.models import Connection
from airflow.settings import Session
import os

def create_snowflake_connection():
    conn = Connection(
        conn_id='snowflake_default',
        conn_type='snowflake',
        host=os.getenv('SNOWFLAKE_ACCOUNT'),
        login=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        schema='staging',
        extra={
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'role': os.getenv('SNOWFLAKE_ROLE'),
        }
    )
    
    session = Session()
    if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
        session.add(conn)
        session.commit()
    session.close()
```

## Summary

| Approach | profiles.yml | Airflow UI | Environment |
|----------|--------------|------------|-------------|
| **Cosmos DAGs** | ❌ Not needed | ✅ Required | Optional |
| **Manual dbt** | ✅ Optional | ❌ Not used | ✅ Can use |
| **Production** | ❌ Avoid | ✅ Best | ✅ Very good |

## Recommended Setup

1. ✅ **For Airflow DAGs**: Use Airflow Connections (already configured!)
2. ✅ **For manual dbt**: Keep `profiles.yml` in project dir (not in `~/.dbt/`)
3. ✅ **For security**: Use environment variables in both

You're already set up correctly! Just create the Airflow connection and you're good to go. 🚀
