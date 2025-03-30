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
    
    # Change to the business_transformations directory
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # Generate documentation
    echo "Generating DBT documentation..."
    dbt docs generate --profiles-dir .
    
    # Serve documentation
    echo "Starting DBT documentation server..."
    echo "Access the documentation at: http://localhost:8080"
    dbt docs serve --profiles-dir . --port 8080
    
    # Return to the original directory (this will only execute after the server is stopped)
    cd ..
    
    echo "==========================================================="
}

# Function to generate DBT docs without serving
generate-dbt-docs() {
    echo "==========================================================="
    echo "Generating DBT documentation..."
    
    # Change to the business_transformations directory
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # Generate documentation
    echo "Generating DBT documentation..."
    dbt docs generate --profiles-dir .
    
    echo "✅ DBT documentation generated successfully."
    echo "You can serve it using the 'serve-dbt-docs' command."
    
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

# Function to start DBT transformations
start-dbt-transformations() {
    echo "==========================================================="
    echo "Starting DBT transformations..."
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Run DBT transformations directly (no container)
    echo "Running DBT transformations using DBT Cloud..."
    
    # Change to the business_transformations directory
    cd business_transformations || { 
        echo "❌ Could not find business_transformations directory."; 
        return 1; 
    }
    
    # First load seeds
    echo "Loading DBT seeds..."
    dbt seed --profiles-dir=.
    
    if [ $? -ne 0 ]; then
        echo "❌ DBT seed loading failed"
        cd ..
        return 1
    fi
    
    echo "✅ DBT seeds loaded successfully."
    
    # Then run the models
    echo "Running DBT models..."
    dbt run --profiles-dir=.
    
    if [ $? -ne 0 ]; then
        echo "❌ DBT run failed"
        cd ..
        return 1
    fi
    
    echo "✅ DBT models run successfully."
    
    # Generate the documentation
    echo "Generating DBT documentation..."
    dbt docs generate --profiles-dir=.
    
    # Return to the original directory
    cd ..
    
    echo "✅ DBT transformations completed successfully."
    echo "You can view your models in the BigQuery console."
    
    echo "==========================================================="
    return 0
} 