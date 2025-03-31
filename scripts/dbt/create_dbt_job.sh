#!/bin/bash

# Script to create a DBT Cloud job for the Agriculture Data Pipeline

# Source the main script to get helper functions
source scripts/main.sh

# Extract values from dbt_cloud.yml
extract_values_from_dbt_cloud_yml() {
    local file_path="${PROJECT_ROOT}/business_transformations/dbt_cloud.yml"
    
    if [ ! -f "$file_path" ]; then
        echo "❌ Error: dbt_cloud.yml file not found at $file_path"
        exit 1
    fi
    
    # Extract values
    token=$(grep -o "token-value: \"[^\"]*\"" "$file_path" | cut -d'"' -f2)
    if [ -z "$token" ]; then
        token=$(grep "token-value:" "$file_path" | awk '{print $2}')
    fi
    
    account_id=$(grep -o "account-id: \"[^\"]*\"" "$file_path" | cut -d'"' -f2)
    if [ -z "$account_id" ]; then
        account_id=$(grep "account-id:" "$file_path" | awk '{print $2}')
    fi
    
    project_id=$(grep -o "project-id: \"[^\"]*\"" "$file_path" | cut -d'"' -f2)
    if [ -z "$project_id" ]; then
        project_id=$(grep "project-id:" "$file_path" | awk '{print $2}')
    fi
    
    # Validate values
    if [ -z "$token" ] || [ -z "$account_id" ] || [ -z "$project_id" ]; then
        echo "❌ Error: Failed to extract all required values from dbt_cloud.yml"
        exit 1
    fi
    
    echo "✅ Successfully extracted values from dbt_cloud.yml"
}

# Function to get environment ID from DBT Cloud
get_environment_id() {
    # Get environments list
    echo "Fetching environments from DBT Cloud..."
    
    environments_response=$(curl -s -X GET \
        -H "Authorization: Token $token" \
        -H "Content-Type: application/json" \
        "https://cloud.getdbt.com/api/v2/accounts/$account_id/environments/?project_id=$project_id")
    
    # Extract the first environment ID (we'll use this one)
    environment_id=$(echo $environments_response | grep -o '"id":[0-9]*,' | head -1 | sed 's/[^0-9]*//g')
    
    if [ -z "$environment_id" ]; then
        echo "❌ Warning: No environment found for this project in DBT Cloud."
        echo "Using null for environment_id, which will use the default environment."
        environment_id="null"
    else
        echo "✅ Found environment ID: $environment_id"
    fi
}

# Function to create a DBT Cloud job
create_dbt_cloud_job() {
    echo "Creating a new DBT Cloud job..."
    
    # Set up the job creation payload
    local payload=$(cat << EOF
{
  "environment_id": $environment_id,
  "name": "Agriculture Data Pipeline - OLAP Processing",
  "dbt_version": null,
  "triggers": {
    "github_webhook": false,
    "git_provider_webhook": false,
    "schedule": true,
    "custom_branch_only": false
  },
  "execute_steps": [
    "dbt build"
  ],
  "settings": {
    "threads": 4,
    "target_name": "dev"
  },
  "state": 1,
  "generate_docs": true,
  "description": "Job to process OLAP data from GCS to BigQuery - Created automatically by the Agriculture Data Pipeline"
}
EOF
)

    # Create the job
    echo "Sending request to create job..."
    
    create_job_response=$(curl -s -X POST \
        -H "Authorization: Token $token" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        "https://cloud.getdbt.com/api/v2/accounts/$account_id/jobs/")
    
    # Extract the job ID from the response
    job_id=$(echo $create_job_response | grep -o '"id":[0-9]*' | head -1 | sed 's/[^0-9]*//g')
    
    if [ -z "$job_id" ]; then
        echo "❌ Failed to create DBT Cloud job."
        echo "Response: $create_job_response"
        exit 1
    else
        echo "✅ Created new DBT Cloud job with ID: $job_id"
        
        # Save the job ID to .env file if it exists
        if [ -f "${PROJECT_ROOT}/.env" ]; then
            # Check if DBT_CLOUD_JOB_ID already exists in .env
            if grep -q "DBT_CLOUD_JOB_ID=" "${PROJECT_ROOT}/.env"; then
                # Update the existing value
                sed -i "s/DBT_CLOUD_JOB_ID=.*/DBT_CLOUD_JOB_ID=$job_id/" "${PROJECT_ROOT}/.env"
            else
                # Add the new value
                echo "DBT_CLOUD_JOB_ID=$job_id" >> "${PROJECT_ROOT}/.env"
            fi
            echo "✅ Updated .env file with DBT_CLOUD_JOB_ID=$job_id"
        fi
    fi
}

# Main execution
echo "==================================================="
echo "Creating DBT Cloud Job for Agriculture Data Pipeline"
echo "==================================================="

# Extract values from dbt_cloud.yml
extract_values_from_dbt_cloud_yml

# Get environment ID
get_environment_id

# Create the job
create_dbt_cloud_job

echo "==================================================="
echo "Job creation completed!"
echo "===================================================" 