"""
Test script for the data storage functionality.
This demonstrates how to fetch and store data from the Global Footprint Network API locally.
"""

import os
import json
import logging
import sys
from pathlib import Path

# Add parent directory to path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from utils.data_storage import FootprintDataStorage
from utils.api_client import FootprintNetworkAPI
from config.settings import LOCAL_RAW_DATA_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('test_data_storage')

def print_json_sample(filepath, max_items=3):
    """Print a sample of a JSON file"""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            sample = data[:max_items]
            logger.info(f"Sample data (first {len(sample)} of {len(data)} items):")
            logger.info(json.dumps(sample, indent=2))
        else:
            logger.info("Data sample:")
            logger.info(json.dumps(data, indent=2))
    except Exception as e:
        logger.error(f"Failed to read JSON file: {str(e)}")


def main():
    """Run the data storage tests"""
    logger.info(f"Testing data storage with raw data path: {LOCAL_RAW_DATA_PATH}")
    
    # Initialize the data storage
    data_storage = FootprintDataStorage()
    
    # Test 1: Fetch and store countries
    logger.info("\n=== TEST 1: Fetching and storing countries ===")
    countries_filepath = data_storage.fetch_and_store_countries()
    logger.info(f"Saved countries data to: {countries_filepath}")
    print_json_sample(countries_filepath)
    
    # Test 2: Fetch and store years
    logger.info("\n=== TEST 2: Fetching and storing years ===")
    years_filepath = data_storage.fetch_and_store_years()
    logger.info(f"Saved years data to: {years_filepath}")
    print_json_sample(years_filepath)
    
    # Test 3: Fetch and store record types
    logger.info("\n=== TEST 3: Fetching and storing record types ===")
    types_filepath = data_storage.fetch_and_store_record_types()
    logger.info(f"Saved record types data to: {types_filepath}")
    print_json_sample(types_filepath)
    
    # Test 4: Fetch and store data for a specific country and year
    logger.info("\n=== TEST 4: Fetching and storing country-year data ===")
    # Using Afghanistan (country code 2) and 2023 for the test
    country_data_filepath = data_storage.fetch_and_store_country_data("2", 2023)
    logger.info(f"Saved country data to: {country_data_filepath}")
    print_json_sample(country_data_filepath)
    
    # Test 5: Fetch and store data for specific record types
    logger.info("\n=== TEST 5: Fetching and storing filtered record types ===")
    # Fetch only BCpc (BiocapPerCap) and pop (Population) records for Afghanistan in 2020
    filtered_data_filepath = data_storage.fetch_and_store_country_data(
        country_code="2", 
        year=2020, 
        record_types=["BCpc", "pop"]
    )
    logger.info(f"Saved filtered data to: {filtered_data_filepath}")
    print_json_sample(filtered_data_filepath)
    
    # Test 6: Fetch data for all countries for a specific year
    logger.info("\n=== TEST 6: Fetching data for all countries for year 2022 ===")
    year_data_filepath = data_storage.fetch_and_store_year_data(2022)
    logger.info(f"Saved year data to: {year_data_filepath}")
    print_json_sample(year_data_filepath)
    
    logger.info("\nAll data storage tests completed!")
    logger.info(f"Raw data files stored in: {LOCAL_RAW_DATA_PATH}")


if __name__ == "__main__":
    main()
