# User Data Anonymization Pipeline
[![CI Status](https://github.com/samuelTyh/user-data-anonymization/actions/workflows/ci.yml/badge.svg)](https://github.com/samuelTyh/user-data-anonymization/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/samuelTyh/user-data-anonymization/graph/badge.svg?token=FYKVODE97R)](https://codecov.io/gh/samuelTyh/user-data-anonymization)

A data pipeline that ingests data from an external API, anonymizes it according to privacy guidelines, stores it securely, and generates analytical reports.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
  - [Using Docker](#using-docker)
  - [Using Python Directly](#using-python-directly)
  - [Command-line Arguments](#command-line-arguments)
- [Project Structure](#project-structure)
- [Pipeline Process](#pipeline-process)
- [Anonymization Rules](#anonymization-rules)
- [Reports Generated](#reports-generated)
- [Testing](#testing)
- [Production Approach](#production-approach)

## Overview

This project implements a data pipeline that:
1. Fetches simulated user data from the Faker API
2. Anonymizes the data following strict privacy guidelines
3. Stores the data in DuckDB or in parquet format
4. Generates reports to answer specific business questions

## Features

- **API Integration**: Robust API client with retry policies, connection pooling, and error handling
- **Data Anonymization**: Implementation of privacy-preserving transformations including:
  - PII masking
  - Data generalization (age groups, email domains)
  - Coordinate fuzzing
- **Efficient Storage**: DuckDB-based storage with SQL query capabilities
- **Reporting**: SQL-based analytics answering specific business questions
- **Containerization**: Docker support for easy deployment
- **Testing**: Comprehensive test suite with high code coverage
- **CI/CD**: GitHub Actions workflow for continuous integration
- **Prefect Integration**: Orchestrator for scheduling, monitoring, and managing the pipeline

## Requirements

- Python 3.12+
- uv
- Docker
- GNU Make

## Installation

### Using uv

```bash
# Clone the repository
git clone https://github.com/samuelTyh/user-data-anonymization.git
cd user-data-anonymization

# Install dependencies
uv venv
uv sync
```

### Using Docker

```bash
# Build the Docker image
make build
```

## Usage

### Using Docker

The easiest way to run the pipeline is using Docker with the provided Makefile:

```bash
# Run the entire pipeline with default settings
make run

# View available commands
make help
```

### Using Python Directly

```bash
# Run the pipeline with default settings
python -m pipeline.main

# Run with custom options
python -m pipeline.main --persons 10000 --gender female --output ./data/my_output.duckdb
```

### Using Prefect

```bash
# Start a Prefect server
make prefect-server

# Deploy the flow with scheduling
make prefect-deploy

# Operate and monitor flows, deployment, and runs in web UI via localhost:4200
```

### Command-line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--persons` | Number of persons to fetch | 30000 |
| `--gender` | Filter by gender (male/female/empty for all) | "" (all) |
| `--output` | Output database path | ./data/anonymization.duckdb |
| `--report` | Output report path | ./data/report.json |

## Project Structure

```
.
├── Dockerfile              # Docker configuration
├── Makefile                # Build and run automation
├── README.md               # Project documentation
├── orchestration/          # NEW: All orchestration-related code
│   ├── __init__.py         # Makes orchestration a proper package
│   ├── prefect_flow.py     # Main Prefect flow definition
│   └── scripts/            # Deployment and execution scripts
│       ├── __init__.py
│       └── deploy.py       # Simple deployment script
├── pipeline                # Main package
│   ├── __init__.py
│   ├── anonymizer.py       # Data anonymization logic
│   ├── api_client.py       # Faker API client
│   ├── config.py           # Configuration management
│   ├── main.py             # Pipeline entry point
│   ├── reporter.py         # Report generation
│   ├── schema.py           # Data schemas and definitions
│   └── storage.py          # DuckDB storage interface
├── pyproject.toml          # Python project configuration
├── requirements.txt        # Dependencies
└── tests                   # Test suite
    ├── __init__.py
    ├── test_anonymizer.py
    ├── test_api_client.py
    ├── test_reporter.py
    ├── test_schema.py
    └── test_storage.py
```

## Pipeline Process

1. **Data Ingestion**:
   - Connects to Faker API
   - Fetches batches of 1000 persons (to manage API limitations)
   - Handles retry logic and connection errors

2. **Anonymization**:
   - Masks PII fields with "****"
   - Generalizes birthdates to 10-year age groups (e.g., [30-40])
   - Keeps only email domains (e.g., gmail.com)
   - Anonymizes coordinates with small random shifts

3. **Storage**:
   - Creates database schema in DuckDB
   - Stores anonymized records efficiently
   - Creates views for reporting

4. **Reporting**:
   - Answers specific business questions:
     - Percentage of users in Germany using Gmail
     - Top 3 countries using Gmail
     - Number of people over 60 years using Gmail
   - Generates additional statistics on email providers, countries, age groups

## Anonymization Rules

The pipeline follows these anonymization guidelines:

| Data Type | Anonymization Method |
|-----------|----------------------|
| Personal Identifiers (name, phone) | Masked with **** |
| Email | Only domain part retained (e.g., gmail.com) |
| Birthdate | Replaced with 10-year age group (e.g., [30-40]) |
| Address | City and country retained, street details masked |
| Coordinates | Fuzzing with small random shifts |

## Reports Generated

The pipeline generates a JSON report with the following information:

1. **Germany Gmail Percentage**: Percentage of users in Germany using Gmail
2. **Top Gmail Countries**: Top 3 countries using Gmail as email provider
3. **Seniors with Gmail**: Count of people over 60 years using Gmail
4. **Email Provider Stats**: Distribution of email providers
5. **Country Stats**: User distribution by country
6. **Age Group Stats**: User distribution by age group

## Testing

The project includes a comprehensive test suite:

```bash
# Run tests
pytest

# Run tests with coverage report
pytest --cov=pipeline

# Run tests in Docker
make test
```

## Production Approach

Based on the current implementation, here are production-ready enhancements aligned with cloud-based solutions. Or we can enhance orchestration, data management, and CI/CD to improve the performance, development efficiency:

### Cloud-Based Infrastructure on AWS

- Replace local storage with **S3** for data storage (raw, anonymized, and reports)
- Implement **Lambda** functions for event-triggered anonymization processing
- Use **Glue** for scalable ETL processing of larger datasets
- Leverage **RDS** for metadata tracking and configuration
- Set up CloudWatch (AWS) metrics and alarms for pipeline performance
- Implement proper logging standards across all components
- Create dashboards for tracking data volume, processing time, and error rates

**Or**

### Workflow Orchestration
- Replace the sequential execution with **Airflow DAGs**
- Implement proper task dependencies for each pipeline stage
- Set up sensors for data availability
- Create dynamic DAG generation based on configuration
- Add proper SLA monitoring and failure handling

### Data Management

1. **Snowflake Integration**:
   - Replace DuckDB with Snowflake for enterprise-scale analytics
   - Leverage Snowflake's data sharing capabilities for secure access
   - Implement proper warehouse sizing strategies for cost optimization
   - Use Snowpipe for continuous data loading

2. **Data Quality Framework**:
   - Implement automated data validation checks pre/post anonymization
   - Add data quality reporting to the pipeline output
   - Set up anomaly detection for unusual patterns
   - Create data quality dashboards for monitoring

### CI/CD and DevOps

1. **GitHub Actions Enhancements**:
   - Expand the current workflow with deployment stages
   - Implement proper environment segregation (dev/staging/prod)
   - Set up automatic versioning and releases

2. **Infrastructure as Code**:
   - Implement IaC for reproducible infrastructure
   - Manage all cloud resources with version-controlled templates
   - Set up proper parameter management for different environments
