# PowerShell setup script for Astronomer Cosmos + dbt + Airflow

Write-Host "🚀 Setting up Astronomer Cosmos for dbt + Airflow..." -ForegroundColor Cyan
Write-Host ""

# Step 1: Stop existing containers
Write-Host "📦 Step 1: Stopping existing containers..." -ForegroundColor Yellow
docker-compose down
Write-Host "✅ Containers stopped" -ForegroundColor Green
Write-Host ""

# Step 2: Rebuild with new dependencies
Write-Host "🏗️  Step 2: Rebuilding Docker images with dbt and Cosmos..." -ForegroundColor Yellow
docker-compose build --no-cache
Write-Host "✅ Images rebuilt" -ForegroundColor Green
Write-Host ""

# Step 3: Start containers
Write-Host "🚀 Step 3: Starting containers..." -ForegroundColor Yellow
docker-compose up -d
Write-Host "✅ Containers started" -ForegroundColor Green
Write-Host ""

# Step 4: Wait for Airflow to be ready
Write-Host "⏳ Step 4: Waiting for Airflow to be ready (this may take 30-60 seconds)..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# Check if webserver is up
$ready = $false
$attempts = 0
$maxAttempts = 12

while (-not $ready -and $attempts -lt $maxAttempts) {
    try {
        docker exec airflow_webserver airflow version 2>&1 | Out-Null
        $ready = $true
    }
    catch {
        Write-Host "   Still waiting..." -ForegroundColor Gray
        Start-Sleep -Seconds 10
        $attempts++
    }
}

if ($ready) {
    Write-Host "✅ Airflow is ready!" -ForegroundColor Green
    Write-Host ""
    
    # Step 5: Verify dbt installation
    Write-Host "🔍 Step 5: Verifying dbt installation..." -ForegroundColor Yellow
    docker exec airflow_webserver dbt --version
    Write-Host "✅ dbt installed successfully" -ForegroundColor Green
    Write-Host ""
    
    # Step 6: Test dbt project
    Write-Host "🧪 Step 6: Testing dbt project..." -ForegroundColor Yellow
    docker exec airflow_webserver dbt debug --project-dir /opt/airflow/ecommerce_transform
    Write-Host ""
    
    Write-Host "✨ Setup complete! ✨" -ForegroundColor Magenta
    Write-Host ""
    Write-Host "📋 Next steps:" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "1. Set up Snowflake connection:" -ForegroundColor White
    Write-Host "   - Go to http://localhost:8080" -ForegroundColor Gray
    Write-Host "   - Login (default: airflow/airflow)" -ForegroundColor Gray
    Write-Host "   - Go to Admin → Connections" -ForegroundColor Gray
    Write-Host "   - Create connection 'snowflake_default' with your credentials" -ForegroundColor Gray
    Write-Host "   - OR run the 'setup_snowflake_connection' DAG (update credentials first!)" -ForegroundColor Gray
    Write-Host ""
    Write-Host "2. Enable your DAG:" -ForegroundColor White
    Write-Host "   - Go to DAGs page" -ForegroundColor Gray
    Write-Host "   - Find 'dbt_ecommerce_simple' or 'dbt_ecommerce_full_pipeline'" -ForegroundColor Gray
    Write-Host "   - Toggle it ON" -ForegroundColor Gray
    Write-Host "   - Click play button to trigger manually" -ForegroundColor Gray
    Write-Host ""
    Write-Host "3. Monitor execution:" -ForegroundColor White
    Write-Host "   - Click on the DAG to see the graph" -ForegroundColor Gray
    Write-Host "   - Each dbt model will be a separate task!" -ForegroundColor Gray
    Write-Host "   - Check logs for detailed output" -ForegroundColor Gray
    Write-Host ""
    Write-Host "📚 See dags/README_COSMOS_SETUP.md for detailed documentation" -ForegroundColor Cyan
    Write-Host ""
}
else {
    Write-Host "❌ Airflow failed to start. Check docker logs:" -ForegroundColor Red
    Write-Host "   docker logs airflow_webserver" -ForegroundColor Gray
    Write-Host "   docker logs airflow_scheduler" -ForegroundColor Gray
}

