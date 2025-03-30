#!/bin/bash

# Load environment variables
if [ ! -f ".env" ]; then
    echo "Error: .env file not found. Please create one from .env.example"
    echo "cp .env.example .env"
    exit 1
fi

source .env

# Function to display a section header
section() {
    echo "========================================"
    echo "  $1"
    echo "========================================"
}

# Start the producer
start_producer() {
    section "Starting Agricultural Data Producer"
    python streaming_pipeline/producer.py &
    PRODUCER_PID=$!
    echo "Producer started with PID: $PRODUCER_PID"
    sleep 2
}

# Start the consumer
start_consumer() {
    section "Starting Agricultural Data Consumer"
    python streaming_pipeline/consumer.py &
    CONSUMER_PID=$!
    echo "Consumer started with PID: $CONSUMER_PID"
    sleep 2
}

# Run the batch pipeline
run_batch_pipeline() {
    section "Running Batch Pipeline for ETL Processing"
    python batch_pipeline/export_to_gcs/pipeline.py
}

# Main execution
main() {
    echo "Starting Agricultural Data Pipeline..."
    
    # Start streaming components
    start_producer
    start_consumer
    
    # Wait for some data to be collected
    echo "Waiting for data collection (30 seconds)..."
    sleep 30
    
    # Run batch pipeline
    run_batch_pipeline
    
    # Cleanup
    echo "Cleaning up processes..."
    kill $PRODUCER_PID $CONSUMER_PID 2>/dev/null
    
    echo "Pipeline execution completed!"
}

# Run the main function
main 