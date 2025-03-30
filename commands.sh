#!/bin/bash
# Agricultural Data Pipeline - Command Shell
# This is a wrapper around the modular script files in scripts/

# DEPRECATION NOTICE
echo -e "\033[1;31mDEPRECATION NOTICE\033[0m"
echo -e "\033[1;31m====================\033[0m"
echo -e "\033[1;33mThis script is deprecated and will be removed in a future version.\033[0m"
echo -e "\033[1;33mPlease use the new unified build script instead:\033[0m"
echo -e "\033[1;36m  ./build.sh\033[0m"
echo -e "\033[1;33mFor more information, see BUILD_README.md\033[0m"
echo -e "\033[1;31m====================\033[0m"
echo "Continuing execution in 5 seconds..."
sleep 5

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