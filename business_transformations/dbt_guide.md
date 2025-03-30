# dbt Guide for Agri Data Pipeline

This guide covers how to set up and run the dbt (Data Build Tool) for the Agri Data Pipeline project.

## Prerequisites

- Python 3.8+
- dbt-bigquery package
- Google Cloud credentials
- Access to the BigQuery project
- dbt Cloud account (required for cloud-based development)

## Configuration

The dbt project is configured through two main files:

1. `dbt_project.yml` - The main project configuration
2. `profiles.yml` - Connection information for BigQuery

### Setting Your Project ID

You can customize the Google Cloud project ID in two ways:

1. Set the `GCP_PROJECT_ID` environment variable:
   ```bash
   export GCP_PROJECT_ID="your-project-id"
   ```

2. Or edit the `business_transformations/profiles.yml` file and change the project ID.

### dbt Cloud Setup

Follow these steps to set up your dbt Cloud project:

1. Sign up/login to dbt Cloud:
   - Go to https://cloud.getdbt.com/
   - Create an account or login if you already have one

2. Create a new project:
   - Click the project dropdown in the top navigation
   - Select "Create New Project"
   - Fill in the project details:
     * Name: "Agri Data Pipeline"
     * Choose BigQuery as your warehouse
     * Follow the prompts to connect your BigQuery credentials

3. Get your project ID:
   - Option 1: Look at the URL when viewing your project
     * Format: `https://cloud.getdbt.com/accounts/[account-id]/projects/[project-id]/`
     * The number after `/projects/` is your project ID
   - Option 2: Check project settings
     * Click on Settings in the left navigation
     * Look for the project ID in the project details

4. Update the configuration:
   - Open `business_transformations/dbt_project.yml`
   - Replace the project-id value with your new ID:
     ```yaml
     dbt-cloud:
         project-id: your-new-project-id
     ```

IMPORTANT: Do not use the project ID 359727 as this belongs to another project.

## Running dbt

There are two ways to run dbt in this project:

### 1. Using the commands.sh script

The `commands.sh` script automatically runs dbt as part of the overall pipeline:

```bash
source commands.sh
start-project  # This will run the entire pipeline including dbt
```

### 2. Running dbt manually

To run dbt manually, navigate to the project root directory:

```bash
# Run all models
dbt run

# Run specific models
dbt run --select staging.stg_farm
dbt run --select core.farm_analytics

# Test your data
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Data Flow

1. The batch pipeline exports data to Google Cloud Storage
2. The pipeline loads data into BigQuery tables
3. dbt transforms the raw BigQuery tables into:
   - Staging models (views)
   - Core models (tables)

## Customizing Models

To customize or add new models:

1. For staging models, create new SQL files in `business_transformations/models/staging/`
2. For core models, create new SQL files in `business_transformations/models/core/`
3. Update schema files as needed

## Troubleshooting

If you encounter issues with dbt:

1. Check your BigQuery permissions
2. Verify the `gcp-creds.json` file is valid
3. Ensure the project ID in `profiles.yml` is correct
4. Look for error messages in the dbt output
5. Run `dbt debug` to check your configuration
6. For dbt Cloud issues:
   - Verify your project ID is correctly set
   - Check your BigQuery connection in dbt Cloud
   - Ensure your dbt Cloud account has access to the project
   - Try refreshing your BigQuery credentials in dbt Cloud 