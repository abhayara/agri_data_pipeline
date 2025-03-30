#!/bin/bash
# Agricultural Data Pipeline - Modular Command Shell
# This is a wrapper around the modular script files in scripts/

# Set project variables
export PROJECT_NAME="agri_data_pipeline"

# Display information about modularization
cat << EOF
===============================================================
Agricultural Data Pipeline - Modular Command Shell
===============================================================
This command shell uses modular scripts for better organization.

The modular scripts are located in the scripts/ directory:
- scripts/spark/functions.sh - Spark-related functions
- scripts/batch/functions.sh - Batch pipeline-related functions
- scripts/dbt/functions.sh - DBT-related functions

For more information, run:
  help
===============================================================
EOF

# Source the modular script
if [ -f "./scripts/main.sh" ]; then
    source ./scripts/main.sh
else
    echo "❌ Could not find modular scripts at ./scripts/main.sh"
    echo "Please ensure the modular scripts are properly installed."
    return 1
fi 