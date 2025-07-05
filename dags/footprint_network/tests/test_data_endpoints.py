"""
Test script for the Global Footprint Network data endpoints.
"""

import sys
import os
from pathlib import Path
import json

# Add parent directory to Python path for imports
parent_dir = Path(__file__).parent.parent
sys.path.append(str(parent_dir))

from utils.api_client import FootprintNetworkAPI
from utils.logging_utils import setup_logger

# Set up logger
logger = setup_logger("test_data_endpoints")

def main():
    """
    Test the data endpoints based on official documentation.
    """
    # Create API client
    api = FootprintNetworkAPI()
    
    try:
        # Test 1: Get types (to verify our authentication and to know available record types)
        logger.info("TEST 1: Getting available record types...")
        types = api.get_types()
        if types:
            logger.info(f"SUCCESS: Retrieved {len(types)} record types.")
            logger.info(f"Sample types: {json.dumps(types[:3], indent=2)}")
            
            # Extract a record type code for later use
            record_type = next((t.get('code') for t in types), 'EFCpc')
            logger.info(f"Selected record type for testing: {record_type}")
        else:
            logger.error("Failed to retrieve record types.")
            record_type = 'EFCpc'  # Default if failed
            
        # Test 2: Get data for specific country and year
        logger.info("\nTEST 2: Getting data for country code '2' (Afghanistan) for year 1999...")
        try:
            # Direct API request to specific endpoint
            data_endpoint = f"/data/2/1999"
            country_year_data = api._make_request('GET', data_endpoint)
            logger.info(f"SUCCESS: Retrieved data for Afghanistan (1999)")
            logger.info(f"Sample data: {json.dumps(country_year_data[:3] if isinstance(country_year_data, list) else country_year_data, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to get data for country '2' for year 1999: {str(e)}")
            
        # Test 3: Get data for specific country, year and record type
        logger.info(f"\nTEST 3: Getting data for country code '2' for year 1999 and record type '{record_type}'...")
        try:
            # Direct API request with record type
            data_endpoint = f"/data/2/1999/{record_type}"
            filtered_data = api._make_request('GET', data_endpoint)
            logger.info(f"SUCCESS: Retrieved filtered data")
            logger.info(f"Filtered data: {json.dumps(filtered_data[:3] if isinstance(filtered_data, list) else filtered_data, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to get filtered data: {str(e)}")
            
        # Test 4: Get data for specific country and all years
        logger.info("\nTEST 4: Getting data for country code '2' for all years...")
        try:
            data_endpoint = f"/data/2/all"
            all_years_data = api._make_request('GET', data_endpoint)
            logger.info(f"SUCCESS: Retrieved data for all years")
            logger.info(f"Sample data (first few entries): {json.dumps(all_years_data[:3] if isinstance(all_years_data, list) else all_years_data, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to get data for all years: {str(e)}")
            
        # Test 5: Get data for all countries for a specific year
        logger.info("\nTEST 5: Getting data for all countries for year 2000...")
        try:
            data_endpoint = f"/data/all/2000"
            all_countries_data = api._make_request('GET', data_endpoint)
            logger.info(f"SUCCESS: Retrieved data for all countries in 2000")
            logger.info(f"Sample data (first few entries): {json.dumps(all_countries_data[:3] if isinstance(all_countries_data, list) else all_countries_data, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to get data for all countries: {str(e)}")
            
        # Test 6: Get multiple record types for a country and year
        logger.info("\nTEST 6: Getting specific record types (BCpc,pop) for country code '2' for year 1998...")
        try:
            data_endpoint = f"/data/2/1998/BCpc,pop"
            multi_record_data = api._make_request('GET', data_endpoint)
            logger.info(f"SUCCESS: Retrieved multiple record types")
            logger.info(f"Data: {json.dumps(multi_record_data[:3] if isinstance(multi_record_data, list) else multi_record_data, indent=2)}")
        except Exception as e:
            logger.error(f"Failed to get multiple record types: {str(e)}")
        
        logger.info("\nAll data endpoint tests completed!")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
