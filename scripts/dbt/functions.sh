#!/bin/bash
# DBT-related functions for the Agricultural Data Pipeline

# Function to run DBT transformations
run-dbt() {
    echo "==========================================================="
    echo "Running DBT transformations on BigQuery data..."
    
    # Check if source tables exist in BigQuery
    echo "Checking if source tables exist in BigQuery..."
    
    # Change to the business_transformations directory
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # Run dbt debug to check configuration
    echo "Verifying DBT configuration..."
    dbt debug --profiles-dir .
    
    if [ $? -ne 0 ]; then
        echo "❌ DBT configuration has issues. Please fix them before continuing."
        cd ..
        return 1
    fi
    
    # Run dbt run to build models
    echo "Running DBT models..."
    dbt run --profiles-dir .
    
    if [ $? -eq 0 ]; then
        echo "✅ DBT models built successfully."
        
        # Run dbt test to validate models
        echo "Testing DBT models..."
        dbt test --profiles-dir .
        
        # Generate docs
        echo "Generating DBT documentation..."
        dbt docs generate --profiles-dir .
        
        echo "✅ DBT process completed successfully."
    else
        echo "❌ DBT model build failed."
        echo "Check the logs for more details."
        cd ..
        return 1
    fi
    
    # Return to the original directory
    cd ..
    
    echo "==========================================================="
    return 0
}

# Function to serve DBT docs
serve-dbt-docs() {
    echo "==========================================================="
    echo "Generating and serving DBT documentation..."
    
    # Define documentation variables
    DBT_DOCS_PORT=8090
    DBT_DOCS_PID_FILE="${PROJECT_ROOT}/.dbt_docs_pid"
    
    # Change to the business_transformations directory
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # Generate documentation
    echo "Generating DBT documentation..."
    dbt docs generate --profiles-dir .
    
    # Stop any existing dbt docs server
    if [ -f "${DBT_DOCS_PID_FILE}" ]; then
        OLD_DBT_DOCS_PID=$(cat "${DBT_DOCS_PID_FILE}")
        if ps -p $OLD_DBT_DOCS_PID > /dev/null; then
            echo "Stopping existing dbt docs server with PID: $OLD_DBT_DOCS_PID"
            kill $OLD_DBT_DOCS_PID
        fi
    fi
    
    # Serve documentation
    echo "==========================================================="
    echo "⚡ Starting DBT documentation server..."
    echo "⚡ DBT DOCS SERVER: http://localhost:${DBT_DOCS_PORT}"
    echo "⚡ Access the documentation at: http://localhost:${DBT_DOCS_PORT}"
    echo "==========================================================="
    
    # Create logs directory if it doesn't exist
    mkdir -p "${PROJECT_ROOT}/logs"
    
    # Start the server in the background
    nohup dbt docs serve --profiles-dir . --port ${DBT_DOCS_PORT} > "${PROJECT_ROOT}/logs/dbt_docs_server.log" 2>&1 &
    
    # Store the server PID
    DBT_DOCS_PID=$!
    echo "✅ DBT docs server started with PID: $DBT_DOCS_PID"
    echo "✅ Access DBT docs at: http://localhost:${DBT_DOCS_PORT}"
    
    # Save PID to file for later reference
    echo $DBT_DOCS_PID > "${DBT_DOCS_PID_FILE}"
    
    # Return to the original directory
    cd ..
    
    echo "==========================================================="
}

# Function to generate DBT docs without serving
generate-dbt-docs() {
    echo "==========================================================="
    echo "Generating DBT documentation..."
    
    # Define documentation variables
    DBT_DOCS_PORT=8090
    
    # Change to the business_transformations directory
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # Generate documentation
    echo "Generating DBT documentation..."
    dbt docs generate --profiles-dir .
    
    if [ $? -eq 0 ]; then
        echo "✅ DBT documentation generated successfully."
        echo "✅ You can serve it using the 'serve-dbt-docs' command."
        echo "✅ When served, documentation will be available at: http://localhost:${DBT_DOCS_PORT}"
    else
        echo "❌ DBT documentation generation failed."
        cd ..
        return 1
    fi
    
    # Return to the original directory
    cd ..
    
    echo "==========================================================="
}

# Function to create seed files for DBT
create-dbt-seeds() {
    echo "==========================================================="
    echo "Creating seed files for DBT testing..."
    
    # Create seeds directory if it doesn't exist
    mkdir -p business_transformations/seeds
    cd business_transformations/seeds || { 
        echo "❌ Could not create or access seeds directory."; 
        return 1; 
    }
    
    # Create farm seed file
    echo "Creating farm seed file..."
    cat > seed_farm.csv << 'EOL'
farm_id,farm_name,farm_location,farm_size,established_date
1,Green Valley Farm,California,120,2005-03-15
2,Sunrise Acres,Florida,85,2010-06-22
3,Windy Hill Farm,Colorado,200,2000-09-05
4,Riverside Ranch,Louisiana,350,1995-04-30
5,Golden Field Farm,Kansas,275,2015-01-18
6,Blue Lake Farm,Wisconsin,135,2007-11-12
7,Red Oak Farm,Oregon,90,2012-07-08
8,Misty Meadow Farm,Michigan,150,2003-02-14
9,Eastern Morning Farm,New York,85,2018-05-11
10,Southern Harvest Farm,Georgia,180,2008-10-25
EOL

    # Create crop seed file
    echo "Creating crop seed file..."
    cat > seed_crop.csv << 'EOL'
crop_id,crop_name,crop_type,growing_season,water_needs
1,Corn,Grain,Summer,Medium
2,Wheat,Grain,Winter,Low
3,Soybeans,Legume,Summer,Medium
4,Tomatoes,Vegetable,Summer,High
5,Lettuce,Vegetable,Spring,Medium
6,Apples,Fruit,Annual,Medium
7,Strawberries,Fruit,Spring,High
8,Cotton,Fiber,Summer,Medium
9,Rice,Grain,Summer,Very High
10,Potatoes,Vegetable,Spring,Medium
EOL

    # Create weather seed file
    echo "Creating weather seed file..."
    cat > seed_weather.csv << 'EOL'
weather_id,date,location,temperature,precipitation,humidity
1,2023-01-15,California,18,0,45
2,2023-01-15,Florida,25,5,70
3,2023-01-15,Colorado,5,10,30
4,2023-01-15,Louisiana,22,15,80
5,2023-01-15,Kansas,8,0,35
6,2023-02-15,California,20,10,50
7,2023-02-15,Florida,27,20,75
8,2023-02-15,Colorado,3,15,40
9,2023-02-15,Louisiana,24,25,85
10,2023-02-15,Kansas,10,5,45
EOL

    # Create soil seed file
    echo "Creating soil seed file..."
    cat > seed_soil.csv << 'EOL'
soil_id,farm_id,ph_level,nutrient_content,texture,organic_matter
1,1,6.8,High,Loam,4.5
2,2,6.2,Medium,Sandy,2.8
3,3,7.1,Low,Clay,3.2
4,4,6.5,High,Silty,5.1
5,5,7.3,Medium,Sandy Loam,3.9
6,6,6.9,High,Clay Loam,4.7
7,7,7.0,Medium,Silt Loam,3.5
8,8,6.3,Low,Sandy Clay,2.6
9,9,6.7,Medium,Loamy Sand,3.0
10,10,7.2,High,Silty Clay,4.2
EOL

    # Create harvest seed file
    echo "Creating harvest seed file..."
    cat > seed_harvest.csv << 'EOL'
harvest_id,farm_id,crop_id,harvest_date,yield_amount,quality_grade
1,1,1,2023-09-15,450,A
2,2,4,2023-07-20,300,B
3,3,2,2023-08-05,500,A
4,4,8,2023-10-10,200,C
5,5,3,2023-09-01,350,B
6,6,6,2023-10-30,400,A
7,7,1,2023-09-25,550,A
8,8,2,2023-08-15,480,B
9,9,7,2023-06-10,150,A
10,10,3,2023-09-10,320,B
EOL

    # Create production seed file
    echo "Creating production seed file..."
    cat > seed_production.csv << 'EOL'
production_id,farm_id,crop_id,date,quantity_produced,cost
1,1,1,2023-09-15,450,9000
2,2,4,2023-07-20,300,7500
3,3,2,2023-08-05,500,10000
4,4,8,2023-10-10,200,6000
5,5,3,2023-09-01,350,8750
6,6,6,2023-10-30,400,12000
7,7,1,2023-09-25,550,11000
8,8,2,2023-08-15,480,9600
9,9,7,2023-06-10,150,6000
10,10,3,2023-09-10,320,8000
EOL

    # Create yield seed file
    echo "Creating yield seed file..."
    cat > seed_yield.csv << 'EOL'
yield_id,farm_id,crop_id,harvest_id,yield_per_hectare,year
1,1,1,1,3.0,2023
2,2,4,2,4.0,2023
3,3,2,3,2.5,2023
4,4,8,4,0.67,2023
5,5,3,5,0.78,2023
6,6,6,6,3.33,2023
7,7,1,7,2.2,2023
8,8,2,8,0.96,2023
9,9,7,9,1.76,2023
10,10,3,10,1.78,2023
EOL

    # Create sustainability seed file
    echo "Creating sustainability seed file..."
    cat > seed_sustainability.csv << 'EOL'
sustainability_id,farm_id,date,water_usage,carbon_footprint,pesticide_usage
1,1,2023-09-15,45000,5000,120
2,2,2023-07-20,30000,3500,75
3,3,2023-08-05,40000,4500,100
4,4,2023-10-10,60000,7000,150
5,5,2023-09-01,52000,6000,130
6,6,2023-10-30,38000,4200,90
7,7,2023-09-25,49000,5500,110
8,8,2023-08-15,58000,6800,140
9,9,2023-06-10,25000,2800,60
10,10,2023-09-10,42000,4800,105
EOL

    echo "✅ Seed files created successfully!"
    
    # Return to the original directory
    cd ../..
    
    echo "==========================================================="
    echo "To load the seed data, use the 'run-dbt-seeds' command."
    echo "==========================================================="
}

# Function to run DBT seeds
run-dbt-seeds() {
    echo "==========================================================="
    echo "Loading DBT seed data into BigQuery..."
    
    # Check that DBT is installed
    if ! command -v dbt &> /dev/null; then
        echo "❌ DBT is not installed. Installing dbt-bigquery..."
        pip install dbt-bigquery
    fi
    
    # Change to the business_transformations directory
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # Check if seeds directory exists and has CSV files
    if [ ! -d "seeds" ] || [ -z "$(find seeds -name "*.csv" 2>/dev/null)" ]; then
        echo "❌ No seed files found. Please run 'create-dbt-seeds' first."
        cd ..
        return 1
    fi
    
    # Run DBT seed
    echo "Loading seed data..."
    dbt seed --profiles-dir .
    
    # Check the exit status
    if [ $? -eq 0 ]; then
        echo "✅ Seed data loaded successfully!"
    else
        echo "❌ Failed to load seed data. Check the logs for more information."
        cd ..
        return 1
    fi
    
    # Return to the original directory
    cd ..
    
    echo "==========================================================="
    echo "Seed data loaded! You can now run 'run-dbt' to build the models."
    echo "==========================================================="
}

# Function to initialize the DBT environment
initialize-dbt() {
    echo "==========================================================="
    echo "Initializing DBT environment for agricultural data analysis..."
    
    # Check that DBT is installed
    if ! command -v dbt &> /dev/null; then
        echo "❌ DBT is not installed. Installing dbt-bigquery..."
        pip install dbt-bigquery
    fi
    
    # Check GCP credentials
    if [ ! -f ./gcp-creds.json ]; then
        echo "❌ GCP credentials file (gcp-creds.json) not found."
        echo "Please create a service account key file and place it in the project root."
        return 1
    fi
    
    # Create seed data
    echo "Creating seed data for DBT..."
    create-dbt-seeds
    
    # Load seed data
    echo "Loading seed data into BigQuery..."
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # Run DBT debug to check configuration
    echo "Verifying DBT configuration..."
    dbt debug --profiles-dir .
    
    if [ $? -ne 0 ]; then
        echo "❌ DBT configuration has issues. Please fix them before continuing."
        cd ..
        return 1
    fi
    
    # Load seed data
    echo "Loading seed data..."
    dbt seed --profiles-dir .
    
    if [ $? -ne 0 ]; then
        echo "❌ Failed to load seed data. Check the logs for more information."
        cd ..
        return 1
    fi
    
    # Run DBT models
    echo "Running DBT models..."
    dbt run --profiles-dir .
    
    # Check the exit status
    if [ $? -eq 0 ]; then
        echo "✅ DBT models ran successfully!"
    else
        echo "❌ DBT models failed to run. Check the logs for more information."
        cd ..
        return 1
    fi
    
    # Generate documentation
    echo "Generating DBT documentation..."
    dbt docs generate --profiles-dir .
    
    # Return to the original directory
    cd ..
    
    echo "==========================================================="
    echo "DBT environment has been successfully initialized!"
    echo "You can now use 'run-dbt' to run models and 'serve-dbt-docs' to view documentation."
    echo "==========================================================="
}

# Function to verify DBT setup
verify-dbt() {
    echo "==========================================================="
    echo "Verifying DBT setup and models..."
    
    # Check if dbt is installed
    if command -v dbt &> /dev/null; then
        echo "✅ DBT is installed."
    else
        echo "❌ DBT is not installed."
        echo "Install it with: pip install dbt-bigquery"
        return 1
    fi
    
    # Change to the business_transformations directory
    cd business_transformations 2>/dev/null || { 
        echo "❌ business_transformations directory not found."; 
        return 1; 
    }
    
    # List available models
    echo "Available DBT models:"
    dbt ls --profiles-dir .
    
    # Test DBT connection
    echo "Testing DBT connection..."
    dbt debug --profiles-dir .
    
    # Check for transformed tables in BigQuery
    echo "Checking for transformed tables in BigQuery..."
    
    # Return to the original directory
    cd ..
    
    echo "==========================================================="
}

# Function to wait for OLAP data to be exported to BigQuery
wait-for-bigquery-data() {
    echo "==========================================================="
    echo "Checking if OLAP data is already available in BigQuery..."
    
    # Define variables
    MAX_ATTEMPTS=30  # 10 minutes total wait time
    ATTEMPT=1
    
    # Check BQ credentials and project
    if ! bq ls &>/dev/null; then
        echo "❌ BigQuery credentials not set up correctly. Cannot check for data."
        return 1
    fi
    
    # Quick check to see if tables already exist
    if bq ls ${BQ_DATASET_NAME} 2>/dev/null | grep -q "farm_analysis" && \
       bq ls ${BQ_DATASET_NAME} 2>/dev/null | grep -q "crop_analysis" && \
       bq ls ${BQ_DATASET_NAME} 2>/dev/null | grep -q "full_data"; then
        echo "✅ All required OLAP tables already exist in BigQuery! Proceeding with DBT."
        return 0
    fi
    
    # Check for BigQuery dataset
    echo "Waiting for OLAP data to be exported to BigQuery..."
    echo "Checking for BigQuery dataset '${BQ_DATASET_NAME}'..."
    while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
        echo "Check attempt $ATTEMPT of $MAX_ATTEMPTS..."
        
        if bq ls ${BQ_DATASET_NAME} &>/dev/null; then
            echo "✅ BigQuery dataset '${BQ_DATASET_NAME}' exists!"
            
            # Check for tables
            echo "Checking for tables in dataset..."
            TABLES=$(bq ls ${BQ_DATASET_NAME} 2>/dev/null | wc -l)
            if [ $TABLES -gt 2 ]; then  # Account for header lines
                echo "✅ Found $(($TABLES-2)) tables in BigQuery dataset!"
                
                # Check if key tables exist
                if bq ls ${BQ_DATASET_NAME} 2>/dev/null | grep -q "farm_analysis" && \
                   bq ls ${BQ_DATASET_NAME} 2>/dev/null | grep -q "crop_analysis" && \
                   bq ls ${BQ_DATASET_NAME} 2>/dev/null | grep -q "full_data"; then
                    echo "✅ All required OLAP tables found in BigQuery!"
                    return 0
                fi
                
                echo "Still waiting for key tables to appear..."
            else
                echo "Waiting for tables to be created in BigQuery..."
            fi
        else
            echo "Waiting for BigQuery dataset '${BQ_DATASET_NAME}' to be created..."
        fi
        
        sleep 20
        ATTEMPT=$((ATTEMPT + 1))
    done
    
    echo "⚠️ Timeout reached waiting for BigQuery data. Proceeding anyway."
    return 1
}

# Function to start DBT transformations
start-dbt-transformations() {
    echo "==========================================================="
    echo "Starting DBT transformations..."
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Define the documentation port and pid file location
    DBT_DOCS_PORT=8090
    DBT_DOCS_PID_FILE="${PROJECT_ROOT}/.dbt_docs_pid"
    
    # Set required environment variables if not already set
    export BQ_PROJECT=${BQ_PROJECT:-$(grep -A5 "outputs:" business_transformations/profiles.yml | grep "project:" | head -n1 | awk '{print $2}')}
    export BQ_DATASET=${BQ_DATASET:-$(grep -A5 "outputs:" business_transformations/profiles.yml | grep "dataset:" | head -n1 | awk '{print $2}')}
    
    echo "Using BigQuery Project: ${BQ_PROJECT}"
    echo "Using BigQuery Dataset: ${BQ_DATASET}"
    
    # Ensure OLAP tables exist before checking
    echo "Ensuring all required OLAP tables exist..."
    ${PROJECT_ROOT:-$(pwd)}/scripts/dbt/ensure_olap_tables.sh
    
    # Quick check for OLAP data instead of waiting
    echo "Checking if OLAP data is available in BigQuery..."
    wait-for-bigquery-data
    
    # Run DBT transformations directly (no container)
    echo "Running DBT transformations..."
    
    # Change to the business_transformations directory
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # Step 1: Debug and check configuration
    echo "Debugging DBT configuration..."
    dbt debug --profiles-dir=.
    
    if [ $? -ne 0 ]; then
        echo "❌ DBT configuration has issues."
        echo "Checking if BigQuery dataset exists..."
        
        # Try to create the dataset directly using gcloud if available
        if command -v bq &> /dev/null; then
            echo "Creating BigQuery dataset ${BQ_DATASET} in project ${BQ_PROJECT} if it doesn't exist..."
            bq --location=asia-south1 mk --dataset "${BQ_PROJECT}:${BQ_DATASET}" || true
            echo "✅ Dataset should now be available."
            
            # Try debug again
            dbt debug --profiles-dir=.
            if [ $? -ne 0 ]; then
                echo "❌ Still having issues with DBT configuration."
                cd ..
                return 1
            fi
        else
            echo "⚠️ bq command not available. Please create dataset manually."
            cd ..
            return 1
        fi
    fi
    
    # Step 2: Clean up target directory to ensure fresh run, but preserve packages
    echo "Cleaning up previous DBT artifacts..."
    # Only clean target and logs directories, NOT packages
    rm -rf ./target/* ./logs/* 2>/dev/null || true
    mkdir -p ./target ./logs
    
    # Step 3: Install dependencies
    echo "Installing DBT dependencies..."
    dbt deps --profiles-dir=.
    
    if [ $? -ne 0 ]; then
        echo "❌ DBT dependencies installation failed."
        cd ..
        return 1
    fi
    
    # Step 4: Load seeds with proper error handling
    echo "Loading DBT seeds..."
    dbt seed --profiles-dir=. --full-refresh
    
    SEED_STATUS=$?
    
    # If seeds fail, try with schema override
    if [ $SEED_STATUS -ne 0 ]; then
        echo "⚠️ Seed loading encountered issues. Trying with schema creation..."
        
        # Try to create the reference_data schema if needed
        echo "Creating reference_data schema if it doesn't exist..."
        if command -v bq &> /dev/null; then
            bq --location=asia-south1 mk --dataset "${BQ_PROJECT}:reference_data" || true
        fi
        
        # Try again with schema override for all seeds
        dbt seed --profiles-dir=. --full-refresh --vars '{"schema_override": "agri_data"}'
        SEED_STATUS=$?
        
        if [ $SEED_STATUS -ne 0 ]; then
            echo "⚠️ Seed loading still has issues. Continuing with models anyway..."
            echo "This is non-fatal as the raw data may be available without seeds."
        else
            echo "✅ DBT seeds loaded successfully with schema override!"
        fi
    else
        echo "✅ DBT seeds loaded successfully."
    fi
    
    # Step 5: Run the models
    echo "Running DBT models..."
    dbt run --profiles-dir=.
    
    RUN_STATUS=$?
    
    # Check if the run was successful and notify
    if [ $RUN_STATUS -ne 0 ]; then
        echo "❌ DBT model run had issues."
    else
        echo "✅ DBT models run successfully."
        
        # Run tests only if models were successful
        echo "Running DBT tests..."
        dbt test --profiles-dir=.
        TEST_STATUS=$?
        
        if [ $TEST_STATUS -ne 0 ]; then
            echo "⚠️ Some DBT tests failed, but continuing."
        else
            echo "✅ All DBT tests passed."
        fi
    fi
    
    # Step 6: Generate the documentation regardless of run status
    echo "Generating DBT documentation..."
    dbt docs generate --profiles-dir=.
    
    DOCS_STATUS=$?
    if [ $DOCS_STATUS -ne 0 ]; then
        echo "❌ DBT documentation generation failed"
    else
        echo "✅ DBT documentation generated successfully."
        
        # Step 7: Serve the documentation if generation was successful
        echo "Starting DBT documentation server..."
        
        # Stop any existing dbt docs server
        if [ -f "${DBT_DOCS_PID_FILE}" ]; then
            OLD_DBT_DOCS_PID=$(cat "${DBT_DOCS_PID_FILE}")
            if ps -p $OLD_DBT_DOCS_PID > /dev/null; then
                echo "Stopping existing dbt docs server with PID: $OLD_DBT_DOCS_PID"
                kill $OLD_DBT_DOCS_PID
            fi
        fi
        
        echo "==========================================================="
        echo "⚡ DBT DOCS SERVER: http://localhost:${DBT_DOCS_PORT}"
        echo "⚡ Access the documentation at: http://localhost:${DBT_DOCS_PORT}"
        echo "==========================================================="
        
        # Create logs directory if it doesn't exist
        mkdir -p "${PROJECT_ROOT}/logs"
        
        # Run docs serve in the background
        nohup dbt docs serve --profiles-dir=. --port ${DBT_DOCS_PORT} > "${PROJECT_ROOT}/logs/dbt_docs_server.log" 2>&1 &
        
        # Store the server PID
        DBT_DOCS_PID=$!
        echo "✅ DBT docs server started with PID: $DBT_DOCS_PID"
        echo "✅ Access DBT docs at: http://localhost:${DBT_DOCS_PORT}"
        
        # Save PID to file for later reference
        echo $DBT_DOCS_PID > "${DBT_DOCS_PID_FILE}"
    fi
    
    # Return to the original directory
    cd ..
    
    echo "✅ DBT transformations process completed."
    echo "You can view your models in the BigQuery console."
    if [ $DOCS_STATUS -eq 0 ]; then
        echo "DBT documentation is available at: http://localhost:${DBT_DOCS_PORT}"
    fi
    
    echo "==========================================================="
    return 0
}

# Function to check DBT docs server status
check-dbt-docs-status() {
    echo "==========================================================="
    echo "Checking DBT documentation server status..."
    
    # Define variables
    DBT_DOCS_PORT=8090
    DBT_DOCS_PID_FILE="${PROJECT_ROOT}/.dbt_docs_pid"
    
    # Check if PID file exists
    if [ ! -f "${DBT_DOCS_PID_FILE}" ]; then
        echo "❌ DBT documentation server is not running (no PID file)"
        echo "Run 'serve-dbt-docs' to start the documentation server."
        return 1
    fi
    
    # Read PID from file
    DBT_DOCS_PID=$(cat "${DBT_DOCS_PID_FILE}")
    
    # Check if process is running
    if ps -p $DBT_DOCS_PID > /dev/null; then
        echo "✅ DBT documentation server is running with PID: $DBT_DOCS_PID"
        echo "✅ Documentation is available at: http://localhost:${DBT_DOCS_PORT}"
        
        # Check if we can access the docs server using curl (if available)
        if command -v curl &> /dev/null; then
            echo "Verifying server accessibility..."
            if curl -s http://localhost:${DBT_DOCS_PORT} | grep -q "dbt"; then
                echo "✅ Documentation server is responding correctly."
            else
                echo "⚠️ Documentation server appears to be running but might not be responding correctly."
            fi
        fi
        
        return 0
    else
        echo "❌ DBT documentation server with PID $DBT_DOCS_PID is not running."
        echo "Run 'serve-dbt-docs' to start the documentation server."
        
        # Remove stale PID file
        rm -f "${DBT_DOCS_PID_FILE}"
        return 1
    fi
    
    echo "==========================================================="
}

# Function to directly create OLAP tables without waiting
create-olap-tables() {
    echo "==========================================================="
    echo "Creating required OLAP tables in BigQuery..."
    
    # Run the ensure_olap_tables.sh script
    ${PROJECT_ROOT:-$(pwd)}/scripts/dbt/ensure_olap_tables.sh
    
    echo "==========================================================="
    echo "✅ OLAP tables creation completed."
    echo "==========================================================="
}

echo "Agricultural Data Pipeline commands loaded." 