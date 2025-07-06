"""
Configuration settings for the Global Footprint Network data ingestion pipeline.
"""

import os
from pathlib import Path

# Try to import dotenv, but don't fail if it's not available
try:
    from dotenv import load_dotenv
    
    # Load environment variables from .env file if it exists
    dotenv_path = Path(__file__).parent.parent / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path=str(dotenv_path))
except ImportError:
    # python-dotenv not available, just continue without loading .env
    print("Warning: python-dotenv not installed. Environment variables from .env will not be loaded.")

# API Configuration
API_BASE_URL = "https://api.footprintnetwork.org/v1"
API_USERNAME = os.environ.get("FOOTPRINT_API_USERNAME", "username")
API_KEY = os.environ.get("FOOTPRINT_API_KEY", "")  # No default value for security

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
