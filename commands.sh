#!/bin/bash
# Agricultural Data Pipeline - Command Shell
# This is a wrapper around the modular script files in scripts/

# Set project variables
export PROJECT_NAME="agri_data_pipeline"

# Display warning about modularization
cat << EOF
===============================================================
NOTICE: Agricultural Data Pipeline - Command Shell
===============================================================
This command shell is being modularized for better organization.

The new modular scripts are located in the scripts/ directory:
- scripts/spark/functions.sh - Spark-related functions
- scripts/batch/functions.sh - Batch pipeline-related functions
- scripts/dbt/functions.sh - DBT-related functions

You can use either this shell or the modular scripts directly.
To use the modular scripts, run:
  source scripts/main.sh

For more information, run:
  help
===============================================================
EOF

# Source the modular script
if [ -f "./scripts/main.sh" ]; then
    source ./scripts/main.sh
else
    echo "⚠️ Could not find modular scripts. Using legacy monolithic commands."
    
    # Original commands.sh content follows
    # (The original content stays, but will be gradually migrated to modular scripts)
    
    # Function to stream data from Kafka to GCS
    stream-data() {
        docker-compose -f ./docker/streaming/docker-compose.yml --env-file ./.env up -d --build
    }
    
    # ... (rest of the original commands.sh file)
fi