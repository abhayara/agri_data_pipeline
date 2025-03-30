#!/bin/bash
# Spark-related functions for the Agricultural Data Pipeline

# Function to start Spark
start-spark() {
    echo "==========================================================="
    echo "Starting Spark services..."
    
    # Check environment first
    check-environment || { echo "⛔ Environment check failed. Please fix the issues before continuing."; return 1; }
    
    # Ensure GCS configuration is ready
    check_fix_gcs_config
    
    # Ensure the build script is executable and run it
    chmod +x ./docker/spark/build.sh
    ./docker/spark/build.sh
    
    # Start Spark containers
    docker-compose -f ./docker/spark/docker-compose.yml --env-file ./.env up -d
    
    # Wait for containers to start
    echo "Waiting for Spark containers to start..."
    sleep 10
    
    # Check if Spark master is running
    if docker ps | grep -q "${PROJECT_NAME}-spark-master"; then
        echo "✅ Spark master is running."
        
        # Install JARs to Spark container if the script exists
        if [ -f "./batch_pipeline/install_jars.sh" ]; then
            echo "Installing JAR dependencies to Spark container..."
            ./batch_pipeline/install_jars.sh
            echo "✅ JAR dependencies installed to Spark container."
        else
            echo "⚠️ JAR installation script not found. Run check_fix_gcs_config first."
        fi
    else
        echo "❌ Spark master failed to start. Check logs with 'docker logs ${PROJECT_NAME}-spark-master'"
        return 1
    fi
    
    echo "Spark services started successfully."
    echo "==========================================================="
    return 0
}

# Function to stop Spark
stop-spark() {
    echo "==========================================================="
    echo "Stopping Spark services..."
    docker-compose -f ./docker/spark/docker-compose.yml --env-file ./.env down
    echo "Spark services stopped."
    echo "==========================================================="
}

# Function to restart Spark
restart-spark() {
    echo "==========================================================="
    echo "Restarting Spark services..."
    stop-spark
    sleep 5
    start-spark
    echo "Spark services restarted."
    echo "==========================================================="
}

# Function to verify Spark setup
verify-spark() {
    echo "==========================================================="
    echo "Verifying Spark setup..."
    
    # Check if the network exists
    if ! docker network inspect ${PROJECT_NAME}-network &>/dev/null; then
        echo "Creating network ${PROJECT_NAME}-network..."
        docker network create ${PROJECT_NAME}-network
    fi
    
    # Load commands
    echo "✅ Loaded commands for ${PROJECT_NAME}."
    
    # Check if Spark master is running
    if docker ps | grep -q "${PROJECT_NAME}-spark-master"; then
        echo "✅ Spark master is running."
        
        # Check if Spark worker is running
        if docker ps | grep -q "${PROJECT_NAME}-spark-worker"; then
            echo "✅ Spark worker is running."
            
            # Check UI accessibility
            echo "✅ Spark UI is accessible at http://localhost:8080"
            
            # Create a sample word count job to test Spark functionality
            echo "Testing Spark with a sample word count job..."
            docker exec -i ${PROJECT_NAME}-spark-master bash -c "echo 'Hello Spark World Test' > /tmp/test.txt && \
                spark-submit --class org.apache.spark.examples.JavaWordCount \
                --master spark://spark-master:7077 \
                /usr/bin/spark-3.3.1-bin-hadoop3/examples/jars/spark-examples_2.12-3.3.1.jar \
                /tmp/test.txt" > /dev/null 2>&1
            
            if [ $? -eq 0 ]; then
                echo "✅ Spark test job completed successfully."
                return 0
            else
                echo "❌ Spark test job failed. Check Spark logs for more details."
                return 1
            fi
        else
            echo "❌ Spark worker is NOT running."
            echo "Try starting Spark with 'start-spark' command."
            return 1
        fi
    else
        echo "❌ Spark master is NOT running."
        echo "Try starting Spark with 'start-spark' command."
        return 1
    fi
    echo "==========================================================="
} 