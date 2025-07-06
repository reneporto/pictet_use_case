"""
Test script for the Global Footprint Network API based on the official documentation.
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
logger = setup_logger("test_v1_api")

def main():
    """
    Test the API client with the documented endpoints.
    """
    # Create API client
    api = FootprintNetworkAPI()
    
    try:
        # Test 1: Get list of countries
        logger.info("TEST 1: Getting list of countries...")
        countries = api.get_countries()
        if countries:
            logger.info(f"SUCCESS: Retrieved {len(countries)} countries.")
            logger.info(f"Sample country: {json.dumps(countries[0], indent=2)}")
        else:
            logger.error("Failed to retrieve any countries.")
        
        # Test 2: Get count of countries
        logger.info("\nTEST 2: Getting count of countries...")
        count = api.get_countries_count()
        logger.info(f"SUCCESS: Retrieved country count: {count}")
        
        # If we have countries, test getting data for a specific country
        if countries:
            # Get the first country in the list
            country = countries[0]
            country_code = country.get('countryCode')
            country_name = country.get('countryName')
            
            # Test 3: Get data for a specific country
            logger.info(f"\nTEST 3: Getting data for country {country_name} (code: {country_code})...")
            try:
                country_data = api.get_country_data(country_code)
                logger.info(f"SUCCESS: Retrieved data for {country_name}")
                logger.info(f"Sample data: {json.dumps(country_data, indent=2)[:500]}...")
            except Exception as e:
                logger.error(f"Failed to get data for country {country_code}: {str(e)}")
            
            # Test 4: Get data for a specific country and year
            current_year = 2019  # Using a sample year that should have data
            logger.info(f"\nTEST 4: Getting data for {country_name} for year {current_year}...")
            try:
                # We'll try to fetch directly from the API with a custom endpoint
                endpoint = f"/countries/{country_code}/{current_year}"
                year_data = api._make_request('GET', endpoint)
                logger.info(f"SUCCESS: Retrieved data for {country_name} for year {current_year}")
                logger.info(f"Sample year data: {json.dumps(year_data, indent=2)[:500]}...")
            except Exception as e:
                logger.error(f"Failed to get data for country {country_code} for year {current_year}: {str(e)}")
        
        logger.info("\nAll API tests completed!")
        
    except Exception as e:
        logger.error(f"API test failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
