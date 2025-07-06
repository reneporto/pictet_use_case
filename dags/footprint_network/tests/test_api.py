"""
Test script for the Global Footprint Network API client.
"""

import sys
import os
import json
from pathlib import Path

# Add parent directory to Python path for imports
parent_dir = Path(__file__).parent.parent
sys.path.append(str(parent_dir))

# Now we can import our modules
from utils.api_client import FootprintNetworkAPI
from utils.logging_utils import setup_logger

# Set up logger
logger = setup_logger("test_api")

def main():
    """
    Test the API client functionality.
    """
    # Create API client
    api = FootprintNetworkAPI()
    
    try:
        # Test getting list of countries
        logger.info("Testing get_countries()...")
        countries = api.get_countries()
        logger.info(f"Successfully retrieved {len(countries)} countries")
        
        # Display first few countries
        for i, country in enumerate(countries[:5]):
            logger.info(f"Country {i+1}: {country}")
        
        # Test getting data for a specific country
        if countries:
            # Get the first country code
            country_code = countries[0].get('code', 'USA')  # Default to USA if code not found
            
            # Test getting country-specific data
            logger.info(f"Testing get_country_data() for {country_code}...")
            country_data = api.get_country_data(country_code)
            logger.info(f"Successfully retrieved data for {country_code}")
            logger.info(f"Data sample: {json.dumps(country_data)[:500]}...")
            
            # Test getting data for a specific year
            year = 2019
            logger.info(f"Testing get_country_data() for {country_code} in {year}...")
            year_data = api.get_country_data(country_code, year)
            logger.info(f"Successfully retrieved data for {country_code} in {year}")
            logger.info(f"Data sample: {json.dumps(year_data)[:500]}...")
            
            # Test getting data for a range of years
            start_year = 2010
            end_year = 2020
            logger.info(f"Testing get_data_by_year_range() for {country_code} from {start_year} to {end_year}...")
            range_data = api.get_data_by_year_range(country_code, start_year, end_year)
            logger.info(f"Successfully retrieved data for {country_code} from {start_year} to {end_year}")
            logger.info(f"Data sample: {json.dumps(range_data)[:500]}...")
        
        # Test getting global data
        logger.info("Testing get_global_data()...")
        global_data = api.get_global_data()
        logger.info("Successfully retrieved global data")
        logger.info(f"Data sample: {json.dumps(global_data)[:500]}...")
        
        logger.info("All API tests completed successfully!")
        
    except Exception as e:
        logger.error(f"API test failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
