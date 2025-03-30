#!/bin/bash
# Main script for the Agricultural Data Pipeline
# This script loads all the modular function files

# Set project root
export PROJECT_ROOT=$(pwd)
export PROJECT_NAME="agri_data_pipeline"

# Load environment variables if available
if [ -f ".env" ]; then
    source .env
fi

# Source all function modules
source_module() {
    local module_path="$1"
    if [ -f "$module_path" ]; then
        source "$module_path"
        if [ $? -eq 0 ]; then
            echo "✅ Loaded module: $module_path"
        else
            echo "❌ Failed to load module: $module_path"
        fi
    else
        echo "⚠️ Module not found: $module_path"
    fi
}

# Source all modules
echo "==========================================================="
echo "Loading Agricultural Data Pipeline modules..."

# Array of module paths
MODULES=(
    "./scripts/streaming/functions.sh"
    "./scripts/spark/functions.sh"
    "./scripts/batch/functions.sh"
    "./scripts/dbt/functions.sh"
    # Add more modules as they are created
)

# Source each module
for module in "${MODULES[@]}"; do
    source_module "$module"
done

# Function to display help information
help() {
    echo "==========================================================="
    echo "Agricultural Data Pipeline - Command Help"
    echo "==========================================================="
    echo "Available commands:"
    echo
    echo "Infrastructure Management:"
    echo "  start-kafka             - Start Kafka and Zookeeper"
    echo "  stop-kafka              - Stop Kafka and Zookeeper"
    echo "  start-spark             - Start Spark master and worker"
    echo "  stop-spark              - Stop Spark services"
    echo "  start-postgres          - Start PostgreSQL and PgAdmin"
    echo "  stop-postgres           - Stop PostgreSQL and PgAdmin"
    echo "  start-metabase          - Start Metabase for visualization"
    echo "  stop-metabase           - Stop Metabase"
    echo "  start-airflow           - Start Airflow for workflow orchestration"
    echo "  stop-airflow            - Stop Airflow"
    echo
    echo "Pipeline Operations:"
    echo "  start-streaming-pipeline - Start the streaming data pipeline"
    echo "  stop-streaming-pipeline  - Stop the streaming data pipeline"
    echo "  verify-kafka            - Verify Kafka setup and create topic if needed"
    echo "  start-batch-pipeline    - Start the batch data processing pipeline"
    echo "  rebuild-and-restart-all - Rebuild and restart all services"
    echo "  status                  - Show status of all pipeline components"
    echo
    echo "Maintenance and Troubleshooting:"
    echo "  check-environment       - Check and validate environment setup"
    echo "  check_fix_kafka_config  - Check and fix Kafka configuration issues"
    echo "  check_fix_gcs_config    - Check and fix GCS connector configuration for Spark"
    echo "  ensure_broker_hostname_resolution - Ensure Kafka broker hostname resolution"
    echo "  restart-kafka-with-config - Restart Kafka with proper configuration"
    echo "  fix-batch-pipeline      - Fix and restart the batch pipeline with GCS connector"
    echo "  git_checkpoint \"Message\" - Create a Git checkpoint with custom message"
    echo
    echo "DBT Operations:"
    echo "  initialize-dbt          - Initialize DBT environment, create and load seeds, run models"
    echo "  run-dbt                 - Run the DBT models"
    echo "  serve-dbt-docs          - Generate and serve DBT documentation"
    echo "  generate-dbt-docs       - Generate DBT documentation without serving"
    echo "  create-dbt-seeds        - Create seed files for DBT testing"
    echo
    echo "Usage example:"
    echo "  source scripts/main.sh"
    echo "  check-environment"
    echo "  rebuild-and-restart-all"
    echo "==========================================================="
}

echo "Agricultural Data Pipeline commands loaded."
echo "Type 'help' to see available commands."
echo "===========================================================" 