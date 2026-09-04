# Fix dbt Setup Issues

## Problem
You saw these errors when running `dbt debug`:
```
profiles.yml file [ERROR not found]
git [ERROR]
```

## Solution

### 1. Add Snowflake Credentials to `.env`

Copy the example file and fill in your credentials:

```powershell
# Copy the example
Copy-Item .env.example .env

# Edit .env and add your Snowflake credentials
notepad .env
```

Your `.env` should look like:
```bash
SNOWFLAKE_ACCOUNT=uoc61155.us-east-1
SNOWFLAKE_USER=your_actual_username
SNOWFLAKE_PASSWORD=your_actual_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ECOMMERCE_DW
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

### 2. Rebuild Docker Containers

```powershell
# Stop containers
docker compose down

# Rebuild with git support
docker compose build --no-cache

# Start containers
docker compose up -d
```

### 3. Verify the Fix

Wait 30 seconds for Airflow to start, then:

```powershell
# Test dbt debug
docker exec airflow_webserver dbt debug --project-dir /opt/airflow/ecommerce_transform

# You should see:
# ✓ profiles.yml file [OK found and valid]
# ✓ Connection test [OK connection ok]
```

### 4. Test dbt Manually

```powershell
# List models
docker exec airflow_webserver dbt ls --project-dir /opt/airflow/ecommerce_transform

# Run a single model
docker exec airflow_webserver dbt run --select stg_customers --project-dir /opt/airflow/ecommerce_transform
```

## What Changed

### ✅ Created `ecommerce_transform/profiles.yml`
- dbt profile configuration
- Uses environment variables for credentials
- Supports dev and prod targets

### ✅ Updated `Dockerfile`
- Added `git` package (required by dbt)

### ✅ Updated `docker-compose.yml`
- Added Snowflake environment variables
- Passes credentials from `.env` to containers

### ✅ Created `.env.example`
- Template for your credentials

## Testing Cosmos

Once the fixes are applied, test your Cosmos DAG:

1. Go to http://localhost:8080
2. Find `dbt_ecommerce_simple`
3. But first, set up the Airflow connection:
   - Admin → Connections → "+"
   - Connection Id: `snowflake_default`
   - Connection Type: `Snowflake`
   - Login: (your username)
   - Password: (your password)
   - Account: `uoc61155.us-east-1`
   - Database: `ECOMMERCE_DW`
   - Schema: `STAGING`
   - Extra: `{"warehouse": "COMPUTE_WH", "role": "ACCOUNTADMIN"}`

4. Enable the DAG and trigger it!

## Why Two Ways to Configure Credentials?

**For manual dbt commands**: Uses `profiles.yml` + environment variables
**For Cosmos/Airflow**: Uses Airflow connections

Both work together seamlessly!

## Still Having Issues?

Check the logs:
```powershell
# Airflow webserver logs
docker logs airflow_webserver --tail 100

# Airflow scheduler logs
docker logs airflow_scheduler --tail 100

# Test dbt connection
docker exec airflow_webserver dbt debug --project-dir /opt/airflow/ecommerce_transform
```

