"""
Test script for the Global Footprint Network years API endpoints.
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
logger = setup_logger("test_years_api")

def main():
    """
    Test the years-related API endpoints.
    """
    # Create API client
    api = FootprintNetworkAPI()
    
    try:
        # Test 1: Get list of years
        logger.info("TEST 1: Getting list of available years...")
        years = api.get_years()
        if years:
            logger.info(f"SUCCESS: Retrieved years data.")
            logger.info(f"Years data: {json.dumps(years, indent=2)}")
        else:
            logger.error("Failed to retrieve years data.")
        
        # Test 2: Get count of years
        logger.info("\nTEST 2: Getting count of available years...")
        count = api.get_years_count()
        logger.info(f"SUCCESS: Retrieved years count: {count}")
        
        logger.info("\nAll year API tests completed!")
        
    except Exception as e:
        logger.error(f"API test failed: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
