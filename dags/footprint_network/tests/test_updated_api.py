"""
Test script specifically for the updated Global Footprint Network API client.
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
logger = setup_logger("test_updated_api")

def main():
    """
    Test the updated API client based on the documentation.
    """
    # Create API client
    api = FootprintNetworkAPI()
    
    try:
        # Test getting available data types
        logger.info("Testing get_types()...")
        types = api.get_types()
        logger.info(f"Response from get_types(): {json.dumps(types)[:500]}...")
        
        # Test getting countries
        logger.info("\nTesting get_countries()...")
        countries = api.get_countries()
        logger.info(f"Response from get_countries(): {json.dumps(countries)[:500]}...")
        
        # If we have countries, test getting data for the first one
        if countries:
            # Use ISO alpha-2 country code (e.g., 'US', 'CA')
            country = countries[0]
            country_code = country.get('isoa2', 'US')  # Use ISO alpha-2 code
            country_name = country.get('countryName', 'Unknown')
            
            logger.info(f"Selected country: {country_name} ({country_code})")
            
            # Test getting country data for a specific year
            logger.info(f"\nTesting get_country_data() for {country_code} in 2019...")
            country_data = api.get_country_data(country_code, "EFCpc", 2019)
            logger.info(f"Response from get_country_data(): {json.dumps(country_data)[:500]}...")
            
            # Test getting data across years (limited range for testing)
            logger.info(f"\nTesting get_data_by_year_range() for {country_code} from 2015 to 2019...")
            range_data = api.get_data_by_year_range(country_code, "EFCpc", 2015, 2019)
            logger.info(f"Response from get_data_by_year_range(): {json.dumps(range_data)[:500]}...")
        
        # Test getting global data
        logger.info("\nTesting get_global_data()...")
        global_data = api.get_global_data("EFCtot", 2019)
        logger.info(f"Response from get_global_data(): {json.dumps(global_data)[:500]}...")
        
        logger.info("\nAll API tests completed successfully!")
        
    except Exception as e:
        logger.error(f"API test failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
