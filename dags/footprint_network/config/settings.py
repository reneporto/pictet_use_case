"""
Configuration settings for the Global Footprint Network data ingestion pipeline.
"""

# API Configuration
API_BASE_URL = "https://api.footprintnetwork.org/v1"
API_USERNAME = "username"  # Replace with your actual username if different
API_KEY = "qmub4lan4698clu1pep591s845lkprn7p1lrj8j16bfksu5cd59"

# Data storage configuration
LOCAL_RAW_DATA_PATH = "/Users/reneporto/pictet/user_case_1/pictet_airflow/aws-mwaa-local-runner/dags/footprint_network/data/raw"
LOCAL_PROCESSED_DATA_PATH = "/Users/reneporto/pictet/user_case_1/pictet_airflow/aws-mwaa-local-runner/dags/footprint_network/data/processed"

# S3 configuration (for production)
S3_BUCKET_NAME = "footprint-network-data"  # This would be your actual bucket name in AWS
S3_RAW_PREFIX = "raw"
S3_PROCESSED_PREFIX = "processed"

# DuckDB configuration (for local testing)
DUCKDB_PATH = "/Users/reneporto/pictet/user_case_1/pictet_airflow/aws-mwaa-local-runner/dags/footprint_network/data/footprint.db"

# AWS configuration (these would be set through environment variables in production)
AWS_REGION = "eu-west-1"  # Change to your preferred region

# Data extraction parameters
START_YEAR = 2010  # Start extracting data from this year
COUNTRIES = []  # Empty list means all countries
