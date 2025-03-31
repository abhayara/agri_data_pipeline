#!/bin/bash
# Script to fix CSV files for proper BigQuery loading

# Set PROJECT_ROOT to the project root directory
export PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
echo "Project root: $PROJECT_ROOT"

# Change to the seeds directory
cd "$PROJECT_ROOT/business_transformations/seeds" || {
    echo "❌ Could not find seeds directory."
    exit 1
}

echo "Fixing CSV files for proper BigQuery loading..."

# Loop through all CSV files
for csv_file in *.csv; do
    if [ -f "$csv_file" ]; then
        echo "Processing $csv_file..."
        
        # Remove trailing blank lines
        sed -i -e :a -e '/^\n*$/{$d;N;ba' -e '}' "$csv_file"
        
        # Remove Windows carriage returns
        tr -d '\r' < "$csv_file" > "${csv_file}.tmp" && mv "${csv_file}.tmp" "$csv_file"
        
        # Ensure the file ends with exactly one newline
        sed -i -e '$a\' "$csv_file"
        
        # If there are any double quotes in the file, ensure they're properly escaped
        if grep -q '"' "$csv_file"; then
            echo "  - Fixing quotes in $csv_file..."
            # Temporary file for sed operations
            tmp_file="${csv_file}.tmp"
            # Escape double quotes by doubling them (CSV standard)
            sed 's/"/\\"/g' "$csv_file" > "$tmp_file"
            mv "$tmp_file" "$csv_file"
        fi
        
        echo "  ✅ Fixed $csv_file"
    fi
done

echo "All CSV files have been fixed!"

# Create a schema.yml file with proper types
echo "Creating schema.yml file with correct types..."

cat > schema.yml << 'EOL'
version: 2

seeds:
  - name: seed_crop
    config:
      column_types:
        crop_id: int64
    description: "Reference data for crops"

  - name: seed_farm
    config:
      column_types:
        farm_id: int64
        farm_size: int64
    description: "Reference data for farms"

  - name: seed_harvest
    config:
      column_types:
        harvest_id: int64
        farm_id: int64
        crop_id: int64
        yield_amount: int64
    description: "Harvest data"

  - name: seed_production
    config:
      column_types:
        production_id: int64
        farm_id: int64
        crop_id: int64
        quantity_produced: int64
        cost: int64
    description: "Production data"

  - name: seed_soil
    config:
      column_types:
        soil_id: int64
        farm_id: int64
        ph_level: float64
        organic_matter: float64
    description: "Soil data for farms"

  - name: seed_sustainability
    config:
      column_types:
        sustainability_id: int64
        farm_id: int64
        water_usage: int64
        carbon_footprint: int64
        pesticide_usage: int64
    description: "Sustainability metrics"

  - name: seed_weather
    config:
      column_types:
        weather_id: int64
        temperature: int64
        precipitation: int64
        humidity: int64
    description: "Weather data"

  - name: seed_yield
    config:
      column_types:
        yield_id: int64
        farm_id: int64
        crop_id: int64
        harvest_id: int64
        yield_per_hectare: float64
        year: int64
    description: "Yield data"
EOL

echo "✅ Schema file created."
echo "You can now run the seed loading script." 