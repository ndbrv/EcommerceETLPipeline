#!/bin/bash
# Setup script for Astronomer Cosmos + dbt + Airflow

set -e

echo "🚀 Setting up Astronomer Cosmos for dbt + Airflow..."
echo ""

# Step 1: Stop existing containers
echo "📦 Step 1: Stopping existing containers..."
docker-compose down
echo "✅ Containers stopped"
echo ""

# Step 2: Rebuild with new dependencies
echo "🏗️  Step 2: Rebuilding Docker images with dbt and Cosmos..."
docker-compose build --no-cache
echo "✅ Images rebuilt"
echo ""

# Step 3: Start containers
echo "🚀 Step 3: Starting containers..."
docker-compose up -d
echo "✅ Containers started"
echo ""

# Step 4: Wait for Airflow to be ready
echo "⏳ Step 4: Waiting for Airflow to be ready (this may take 30-60 seconds)..."
sleep 30

# Check if webserver is up
until docker exec airflow_webserver airflow version &> /dev/null
do
    echo "   Still waiting..."
    sleep 10
done
echo "✅ Airflow is ready!"
echo ""

# Step 5: Verify dbt installation
echo "🔍 Step 5: Verifying dbt installation..."
docker exec airflow_webserver dbt --version
echo "✅ dbt installed successfully"
echo ""

# Step 6: Test dbt project
echo "🧪 Step 6: Testing dbt project..."
docker exec airflow_webserver dbt debug --project-dir /opt/airflow/ecommerce_transform
echo ""

echo "✨ Setup complete! ✨"
echo ""
echo "📋 Next steps:"
echo ""
echo "1. Set up Snowflake connection:"
echo "   - Go to http://localhost:8080"
echo "   - Login (default: airflow/airflow)"
echo "   - Go to Admin → Connections"
echo "   - Create connection 'snowflake_default' with your credentials"
echo "   - OR run the 'setup_snowflake_connection' DAG (update credentials first!)"
echo ""
echo "2. Enable your DAG:"
echo "   - Go to DAGs page"
echo "   - Find 'dbt_ecommerce_simple' or 'dbt_ecommerce_full_pipeline'"
echo "   - Toggle it ON"
echo "   - Click play button to trigger manually"
echo ""
echo "3. Monitor execution:"
echo "   - Click on the DAG to see the graph"
echo "   - Each dbt model will be a separate task!"
echo "   - Check logs for detailed output"
echo ""
echo "📚 See dags/README_COSMOS_SETUP.md for detailed documentation"
echo ""

