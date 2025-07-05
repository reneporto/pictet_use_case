"""
Comprehensive test script for the Global Footprint Network API integration.
This tests all endpoints and functionality of the API client.
"""

import sys
import os
from pathlib import Path
import json
from pprint import pprint

# Add parent directory to Python path for imports
parent_dir = Path(__file__).parent.parent
sys.path.append(str(parent_dir))

from utils.api_client import FootprintNetworkAPI
from utils.logging_utils import setup_logger

# Set up logger
logger = setup_logger("test_complete_api")

def main():
    """
    Test all API client functionality comprehensively.
    """
    # Create API client
    api = FootprintNetworkAPI()
    
    try:
        logger.info("=== COMPREHENSIVE API TESTING ===")
        
        # TEST 1: Get types
        logger.info("\n=== TEST 1: Record Types ===")
        types = api.get_types()
        logger.info(f"Retrieved {len(types)} record types")
        logger.info(f"Sample types: {json.dumps(types[:3], indent=2)}")
        
        types_count = api.get_types_count()
        logger.info(f"Types count: {types_count}")
        
        # TEST 2: Get countries
        logger.info("\n=== TEST 2: Countries ===")
        countries = api.get_countries()
        logger.info(f"Retrieved {len(countries)} countries")
        logger.info(f"Sample countries: {json.dumps(countries[:3], indent=2)}")
        
        countries_count = api.get_countries_count()
        logger.info(f"Countries count: {countries_count}")
        
        # Find country code for Afghanistan for later use
        afghanistan = next((c for c in countries if c.get('shortName') == 'Afghanistan'), None)
        if afghanistan:
            afghanistan_code = afghanistan.get('countryCode')
            afghanistan_iso = afghanistan.get('isoa2')
            logger.info(f"Found Afghanistan with code {afghanistan_code} and ISO: {afghanistan_iso}")
        else:
            afghanistan_code = 2  # Default from documentation if not found
            logger.warning(f"Afghanistan not found in countries list, using default code {afghanistan_code}")
        
        # TEST 3: Get country data
        logger.info(f"\n=== TEST 3: Single Country Data ===")
        country_data = api.get_country_data(afghanistan_code)
        logger.info(f"Retrieved data for country {afghanistan_code}")
        logger.info(f"Country data: {json.dumps(country_data, indent=2) if isinstance(country_data, dict) else 'Too large to display'}")
        
        # TEST 4: Get years
        logger.info("\n=== TEST 4: Years ===")
        years = api.get_years()
        logger.info(f"Retrieved {len(years)} years")
        logger.info(f"Available years: {json.dumps(years, indent=2)}")
        
        years_count = api.get_years_count()
        logger.info(f"Years count: {years_count}")
        
        # Use the most recent year for testing
        recent_year = years[0]['year'] if years else 2020
        logger.info(f"Using {recent_year} as recent year for testing")
        
        # TEST 5: Get data for specific country and year
        logger.info(f"\n=== TEST 5: Data for Country and Year ===")
        country_year_data = api.get_data_for_country_year(afghanistan_code, recent_year)
        logger.info(f"Retrieved {len(country_year_data) if isinstance(country_year_data, list) else 'N/A'} records")
        logger.info(f"Sample data: {json.dumps(country_year_data[:3] if isinstance(country_year_data, list) else country_year_data, indent=2)}")
        
        # TEST 6: Get data for specific country, year and record type
        logger.info(f"\n=== TEST 6: Data for Country, Year and Record Type ===")
        # Use BCpc and pop record types as examples
        record_types = ['BCpc', 'pop']
        filtered_data = api.get_data_for_record_type(afghanistan_code, recent_year, record_types)
        logger.info(f"Retrieved {len(filtered_data) if isinstance(filtered_data, list) else 'N/A'} records")
        logger.info(f"Filtered data: {json.dumps(filtered_data, indent=2)}")
        
        # TEST 7: Get data for country across all years
        logger.info(f"\n=== TEST 7: Data for Country Across All Years ===")
        all_years_data = api.get_data_for_country_year(afghanistan_code, 'all')
        logger.info(f"Retrieved {len(all_years_data) if isinstance(all_years_data, list) else 'N/A'} records")
        logger.info(f"Sample data: {json.dumps(all_years_data[:3] if isinstance(all_years_data, list) else all_years_data, indent=2)}")
        
        logger.info("\n=== ALL API TESTS COMPLETED SUCCESSFULLY! ===")
        
    except Exception as e:
        logger.error(f"API test failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
