#!/bin/bash
# Script to run the full dbt workflow in the correct sequence

# Set PROJECT_ROOT to the project root directory
export PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
echo "Project root: $PROJECT_ROOT"

# Define variables
DBT_PROJECT_DIR="$PROJECT_ROOT/business_transformations"
DBT_DOCS_PID_FILE="${PROJECT_ROOT}/.dbt_docs_pid"
DBT_DOCS_PORT=8090

# Source the functions file
source "$PROJECT_ROOT/scripts/dbt/functions.sh"

# Set required environment variables from profiles.yml
export BQ_PROJECT=${BQ_PROJECT:-$(grep -A5 "outputs:" $PROJECT_ROOT/business_transformations/profiles.yml | grep "project:" | head -n1 | awk '{print $2}')}
export BQ_DATASET=${BQ_DATASET:-$(grep -A5 "outputs:" $PROJECT_ROOT/business_transformations/profiles.yml | grep "dataset:" | head -n1 | awk '{print $2}')}

echo "Using BigQuery Project: ${BQ_PROJECT}"
echo "Using BigQuery Dataset: ${BQ_DATASET}"

# Change to the dbt project directory
cd "$PROJECT_ROOT/business_transformations"

# Run the full dbt sequence
echo "Starting complete DBT workflow..."

# Create directories if they don't exist
mkdir -p "$PROJECT_ROOT/logs"

# Ensure BigQuery dataset exists
if command -v bq &> /dev/null; then
    echo "Ensuring BigQuery dataset ${BQ_DATASET} exists..."
    bq --location=asia-south1 mk --dataset "${BQ_PROJECT}:${BQ_DATASET}" || true
    
    # Also ensure reference_data dataset exists for seed schema overrides
    echo "Ensuring reference_data dataset exists..."
    bq --location=asia-south1 mk --dataset "${BQ_PROJECT}:reference_data" || true
fi

# Clean up any previous artifacts
echo "Cleaning up previous DBT artifacts..."
dbt clean --profiles-dir .

# Initialize dbt and check connections
echo "Running dbt debug..."
dbt debug --profiles-dir .

if [ $? -ne 0 ]; then
    echo "⚠️ DBT configuration has issues. Continuing anyway as some issues might resolve during execution."
fi

# Install dependencies
echo "Installing dependencies..."
dbt deps --profiles-dir .

# Load seed data with retries
echo "Loading seed data..."
dbt seed --profiles-dir . --full-refresh

if [ $? -ne 0 ]; then
    echo "⚠️ Initial seed loading failed. Trying with schema override..."
    dbt seed --profiles-dir . --full-refresh --vars '{"schema_override": "agri_data"}'
    
    if [ $? -ne 0 ]; then
        echo "⚠️ Seed loading still has issues. Continuing with models anyway."
        echo "This is non-fatal as the raw data may be available without seeds."
    else
        echo "✅ Seeds loaded successfully with schema override."
    fi
else
    echo "✅ Seeds loaded successfully."
fi

# Run models
echo "Running dbt models..."
dbt run --profiles-dir .

RUN_STATUS=$?
if [ $RUN_STATUS -ne 0 ]; then
    echo "⚠️ Some DBT models failed to build. Continuing with documentation."
else 
    echo "✅ DBT models built successfully."
    
    # Test models
    echo "Testing dbt models..."
    dbt test --profiles-dir .
    
    if [ $? -ne 0 ]; then
        echo "⚠️ Some DBT tests failed, but continuing."
    else
        echo "✅ All DBT tests passed."
    fi
fi

# Generate documentation
echo "Generating dbt documentation..."
dbt docs generate --profiles-dir .

if [ $? -ne 0 ]; then
    echo "⚠️ DBT documentation generation failed."
else
    # Serve documentation
    echo "Starting dbt docs server on port ${DBT_DOCS_PORT}..."
    # Stop any existing dbt docs server
    if [ -f "${DBT_DOCS_PID_FILE}" ]; then
        DBT_DOCS_PID=$(cat "${DBT_DOCS_PID_FILE}")
        if ps -p $DBT_DOCS_PID > /dev/null; then
            echo "Stopping existing dbt docs server with PID: $DBT_DOCS_PID"
            kill $DBT_DOCS_PID
        fi
    fi
    
    # Start the docs server in the background
    nohup dbt docs serve --profiles-dir . --port ${DBT_DOCS_PORT} > "${PROJECT_ROOT}/logs/dbt_docs_server.log" 2>&1 &
    DBT_DOCS_PID=$!
    echo "✅ dbt docs server started with PID: $DBT_DOCS_PID"
    echo "✅ You can access dbt docs at: http://localhost:${DBT_DOCS_PORT}"
    
    # Save the PID for later
    echo $DBT_DOCS_PID > "${DBT_DOCS_PID_FILE}"
fi

# Return to original directory
cd "$PROJECT_ROOT"

echo "==========================================================="
echo "DBT workflow completed!"
if [ $RUN_STATUS -eq 0 ]; then
    echo "✅ All DBT models built successfully."
else
    echo "⚠️ Some DBT models had issues. Check the logs for details."
fi
echo "==========================================================="

# Exit with the status of the model run
exit $RUN_STATUS 