#!/bin/bash
# Script to load dbt seed data with enhanced error handling

# Set PROJECT_ROOT to the project root directory
export PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
echo "Project root: $PROJECT_ROOT"

# Set required environment variables from profiles.yml
export BQ_PROJECT=${BQ_PROJECT:-$(grep -A5 "outputs:" $PROJECT_ROOT/business_transformations/profiles.yml | grep "project:" | head -n1 | awk '{print $2}')}
export BQ_DATASET=${BQ_DATASET:-$(grep -A5 "outputs:" $PROJECT_ROOT/business_transformations/profiles.yml | grep "dataset:" | head -n1 | awk '{print $2}')}

echo "Using BigQuery Project: ${BQ_PROJECT}"
echo "Using BigQuery Dataset: ${BQ_DATASET}"

# Source the functions file
source "$PROJECT_ROOT/scripts/dbt/functions.sh"

# Create logs directory if it doesn't exist
mkdir -p "$PROJECT_ROOT/logs"

# Change to the dbt project directory
cd "$PROJECT_ROOT/business_transformations" || {
    echo "❌ Could not find business_transformations directory."
    exit 1
}

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

# Debug first to check configuration
echo "Debugging DBT configuration..."
dbt debug --profiles-dir .

# Load seed data with multiple attempts
echo "Loading DBT seed data with multiple strategies..."

# Attempt 1: Standard loading
echo "Attempt 1: Standard seed loading..."
dbt seed --profiles-dir . --full-refresh
ATTEMPT1_STATUS=$?

if [ $ATTEMPT1_STATUS -ne 0 ]; then
    echo "⚠️ Standard seed loading failed. Trying with schema override..."
    
    # Attempt 2: With schema override
    dbt seed --profiles-dir . --full-refresh --vars '{"schema_override": "agri_data"}'
    ATTEMPT2_STATUS=$?
    
    if [ $ATTEMPT2_STATUS -ne 0 ]; then
        echo "⚠️ Schema override attempt failed. Trying with full reset..."
        
        # Attempt 3: Reset and try again
        dbt clean --profiles-dir .
        dbt seed --profiles-dir . --full-refresh
        ATTEMPT3_STATUS=$?
        
        if [ $ATTEMPT3_STATUS -ne 0 ]; then
            echo "❌ All seed loading attempts failed. Check logs for details."
            echo "This may require manual intervention to fix BigQuery permissions or schema issues."
            exit 1
        else
            echo "✅ Seed loading succeeded after full reset!"
        fi
    else
        echo "✅ Seed loading succeeded with schema override!"
    fi
else
    echo "✅ Seed loading succeeded on first attempt!"
fi

# Return to original directory
cd "$PROJECT_ROOT"

echo "==========================================================="
echo "DBT seed loading process completed successfully!"
echo "You can now run the DBT models with './scripts/dbt/run-dbt.sh'"
echo "==========================================================="

exit 0 