# PowerShell script to create Snowflake connection in Airflow

Write-Host "🔧 Setting up Airflow Snowflake Connection..." -ForegroundColor Cyan

# Python script to create connection (saved as separate file to avoid parsing issues)
$pythonScript = 'from airflow.models import Connection
from airflow.settings import Session
import os

def create_snowflake_connection():
    # Get credentials from environment
    account = os.getenv("SNOWFLAKE_ACCOUNT", "uoc61155.us-east-1")
    user = os.getenv("SNOWFLAKE_USER", "NDBRV3")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    if not password:
        raise SystemExit("SNOWFLAKE_PASSWORD is not set. Export it or add it to .env before running this script.")
    role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    database = os.getenv("SNOWFLAKE_DATABASE", "ECOMMERCE_DW")
    
    conn = Connection(
        conn_id="snowflake_default",
        conn_type="snowflake",
        host=account,
        login=user,
        password=password,
        schema="staging",
        extra={
            "account": account,
            "warehouse": warehouse,
            "database": database,
            "role": role,
        }
    )
    
    session = Session()
    
    # Check if connection exists
    existing = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    if existing:
        print(f"Connection {conn.conn_id} already exists, updating...")
        existing.host = conn.host
        existing.login = conn.login
        existing.password = conn.password
        existing.schema = conn.schema
        existing.extra = conn.extra
    else:
        print(f"Creating new connection {conn.conn_id}...")
        session.add(conn)
    
    session.commit()
    session.close()
    print("✅ Snowflake connection configured!")

if __name__ == "__main__":
    create_snowflake_connection()
'

# Write Python script to temp file
$tempFile = Join-Path $env:TEMP "create_airflow_connection.py"
$pythonScript | Out-File -FilePath $tempFile -Encoding utf8 -Force

Write-Host "✅ Created Python script" -ForegroundColor Green

# Check if Docker containers are running
Write-Host "🔍 Checking if Airflow is running..." -ForegroundColor Yellow
$containerRunning = docker ps --filter "name=airflow_webserver" --filter "status=running" --format "{{.Names}}" 2>$null

if (-not $containerRunning) {
    Write-Host "❌ Airflow webserver container is not running!" -ForegroundColor Red
    Write-Host "   Please start Airflow first:" -ForegroundColor Yellow
    Write-Host "   docker compose up -d" -ForegroundColor Gray
    Remove-Item $tempFile
    exit 1
}

Write-Host "✅ Airflow is running" -ForegroundColor Green

# Copy script to container and execute
Write-Host "📝 Creating Snowflake connection in Airflow..." -ForegroundColor Yellow

try {
    docker cp $tempFile airflow_webserver:/tmp/create_connection.py
    docker exec airflow_webserver python /tmp/create_connection.py
    Write-Host "✅ Connection created successfully" -ForegroundColor Green
}
catch {
    Write-Host "❌ Failed to create connection: $_" -ForegroundColor Red
    Remove-Item $tempFile
    exit 1
}

# Cleanup
Remove-Item $tempFile

Write-Host ""
Write-Host "✨ Setup complete! ✨" -ForegroundColor Magenta
Write-Host ""
Write-Host "🌐 Access Airflow at: http://localhost:8080" -ForegroundColor Cyan
Write-Host "   Username: airflow" -ForegroundColor Gray
Write-Host "   Password: airflow" -ForegroundColor Gray
Write-Host ""
Write-Host "📊 Check your connection:" -ForegroundColor Cyan
Write-Host "   Admin → Connections → snowflake_default" -ForegroundColor Gray
