# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

## [1.3.0] - 2024-03-25

### Added

- Batch pipeline components for processing agricultural data
- Spark transformation pipeline for data processing
- BigQuery data exporters for dimensions and facts tables
- Integration of batch pipeline functionality in unified commands script
- Metadata and trigger configuration for batch pipeline
- Enhanced service coordination between Spark, Airflow, and BigQuery

### Changed

- Unified command-line interface for all pipeline operations
- Enhanced environment variable configuration for batch processing

## [1.0.0] - 2024-03-25

### Added

- Centralized environment variables file with standardized configuration
- Comprehensive README with architecture overview and improvement details
- Health checks for all services (Airflow, Kafka, Postgres, Spark, Metabase)
- Dependency conditions for proper service startup order
- Batch processing pipeline with Spark integration
- GCS integration for data storage
- Graceful shutdown handling for streaming components
- Support for transaction-based processing
- Batch processing in Kafka consumer
- Automated startup sequence with environment validation

### Changed

- Refactored streaming services for better organization
- Enhanced Airflow DAGs with retry mechanisms
- Updated requirements with tenacity and error handling dependencies
- Improved error logging across all components
- Updated Docker Compose configurations for service dependencies
- Standardized environment variable usage

### Fixed

- Solved potential race conditions during service startup
- Addressed potential data loss scenarios with batch processing
- Fixed error handling in Kafka consumers and producers
- Resolved potential port conflicts between services
- Improved credential management for GCP services

## [0.1.0] - 2024-03-20

### Added

- Initial project structure
- Basic Kafka streaming pipeline
- Airflow integration
- Postgres database setup
- Docker containerization 