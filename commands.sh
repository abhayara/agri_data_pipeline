#!/bin/bash

# Agricultural Data Pipeline Commands
# ----------------------------------

# Load environment variables if .env exists
if [ -f .env ]; then
    source .env
    echo "Loaded environment variables from .env"
else
    echo "Warning: .env file not found. Using default values."
fi

# Set default project name if not set
if [ -z "$PROJECT_NAME" ]; then
    export PROJECT_NAME="agri_data_pipeline"
fi

# Function to display a section header
section() {
    echo "========================================"
    echo "  $1"
    echo "========================================"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to install Python dependencies
install_dependencies() {
    section "Installing Python Dependencies"
    
    if ! command_exists pip; then
        echo "Error: pip is not installed. Please install Python and pip first."
        return 1
    fi
    
    echo "Installing required Python packages..."
    pip install -r requirements.txt
    
    if [ $? -eq 0 ]; then
        echo "Successfully installed dependencies."
    else
        echo "Error installing dependencies. Please check the output above."
        return 1
    fi
}

# Function to check if Kafka is running
check_kafka() {
    section "Checking Kafka Status"
    
    # Try to connect to Kafka
    if command_exists nc; then
        if nc -z localhost 9092 >/dev/null 2>&1; then
            echo "Kafka is running on port 9092."
            return 0
        else
            echo "Kafka is not running on port 9092."
            return 1
        fi
    else
        echo "Warning: 'nc' command not found. Cannot check Kafka status."
        return 1
    fi
}

# Function to setup local environment
setup_environment() {
    section "Setting Up Environment"
    
    # Create .env file if it doesn't exist
    if [ ! -f .env ]; then
        echo "Creating .env file from .env.example"
        cp .env.example .env
        echo "Please edit .env file with your configuration values."
    fi
    
    # Install dependencies
    install_dependencies
    
    echo "Environment setup complete."
}

# Function to start the producer
start_producer() {
    section "Starting Agricultural Data Producer"
    
    # Check if dependencies are installed
    if ! python -c "import confluent_kafka" >/dev/null 2>&1; then
        echo "Error: confluent_kafka module not found. Installing dependencies..."
        install_dependencies
    fi
    
    echo "Starting Kafka producer..."
    python streaming_pipeline/producer.py &
    PRODUCER_PID=$!
    echo "Producer started with PID: $PRODUCER_PID"
    
    # Export the PID for later use
    export PRODUCER_PID=$PRODUCER_PID
}

# Function to start the consumer
start_consumer() {
    section "Starting Agricultural Data Consumer"
    
    # Check if dependencies are installed
    if ! python -c "import confluent_kafka, google.cloud" >/dev/null 2>&1; then
        echo "Error: Required modules not found. Installing dependencies..."
        install_dependencies
    fi
    
    echo "Starting Kafka consumer..."
    python streaming_pipeline/consumer.py &
    CONSUMER_PID=$!
    echo "Consumer started with PID: $CONSUMER_PID"
    
    # Export the PID for later use
    export CONSUMER_PID=$CONSUMER_PID
}

# Function to start the streaming pipeline
start_streaming() {
    section "Starting Streaming Pipeline"
    
    # Start producer and consumer
    start_producer
    start_consumer
    
    echo "Streaming pipeline started."
    echo "Use 'stop_streaming' to stop the streaming pipeline."
}

# Function to stop the streaming pipeline
stop_streaming() {
    section "Stopping Streaming Pipeline"
    
    # Kill producer and consumer if they're running
    if [ ! -z "$PRODUCER_PID" ]; then
        echo "Stopping producer (PID: $PRODUCER_PID)..."
        kill $PRODUCER_PID 2>/dev/null
        unset PRODUCER_PID
    fi
    
    if [ ! -z "$CONSUMER_PID" ]; then
        echo "Stopping consumer (PID: $CONSUMER_PID)..."
        kill $CONSUMER_PID 2>/dev/null
        unset CONSUMER_PID
    fi
    
    echo "Streaming pipeline stopped."
}

# Function to run the batch pipeline
run_batch_pipeline() {
    section "Running Batch Pipeline"
    
    # Check if dependencies are installed
    if ! python -c "import pyspark" >/dev/null 2>&1; then
        echo "Error: pyspark module not found. Installing dependencies..."
        install_dependencies
    fi
    
    echo "Running batch pipeline for ETL processing..."
    python batch_pipeline/export_to_gcs/pipeline.py
    
    if [ $? -eq 0 ]; then
        echo "Batch pipeline completed successfully."
    else
        echo "Error running batch pipeline. Check the output above."
    fi
}

# Function to run the full pipeline
run_pipeline() {
    section "Running Full Pipeline"
    
    # Install dependencies if needed
    install_dependencies
    
    # Start streaming
    start_streaming
    
    # Wait for some data to be collected
    echo "Waiting for data collection (30 seconds)..."
    sleep 30
    
    # Run batch pipeline
    run_batch_pipeline
    
    # Stop streaming
    stop_streaming
    
    echo "Full pipeline execution completed!"
}

# Function to clean up all processes
cleanup() {
    section "Cleaning Up"
    
    # Stop streaming
    stop_streaming
    
    echo "Cleanup completed."
}

# Display available commands
show_help() {
    section "Agricultural Data Pipeline Commands"
    echo 
    echo "Available commands:"
    echo "  setup_environment   - Set up the environment and install dependencies"
    echo "  install_dependencies - Install Python dependencies"
    echo "  start_producer      - Start the Kafka producer"
    echo "  start_consumer      - Start the Kafka consumer"
    echo "  start_streaming     - Start both producer and consumer"
    echo "  stop_streaming      - Stop the streaming pipeline"
    echo "  run_batch_pipeline  - Run the batch ETL pipeline"
    echo "  run_pipeline        - Run the full pipeline (streaming + batch)"
    echo "  cleanup             - Clean up all processes"
    echo 
    echo "Example: source commands.sh && setup_environment && run_pipeline"
}

# Show help by default when the script is sourced
show_help

echo "Agricultural Data Pipeline commands loaded."
echo "Type 'help' to see available commands."

# Define help command
help() {
    show_help
}

# Export all functions
export -f section
export -f command_exists
export -f install_dependencies
export -f check_kafka
export -f setup_environment
export -f start_producer
export -f start_consumer
export -f start_streaming
export -f stop_streaming
export -f run_batch_pipeline
export -f run_pipeline
export -f cleanup
export -f show_help
export -f help