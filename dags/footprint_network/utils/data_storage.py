"""
Data storage utilities for the Global Footprint Network API.
This module handles fetching data from the API and storing it locally.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

from utils.api_client import FootprintNetworkAPI
from config.settings import LOCAL_RAW_DATA_PATH

# Configure logging
logger = logging.getLogger(__name__)


class FootprintDataStorage:
    """
    Class to handle data retrieval from the API and local storage.
    """
    def __init__(self, api_client: Optional[FootprintNetworkAPI] = None, 
                 raw_data_path: str = LOCAL_RAW_DATA_PATH):
        """
        Initialize the FootprintDataStorage with an API client and path for raw data.
        
        Args:
            api_client: FootprintNetworkAPI instance, creates a new one if None
            raw_data_path: Path to store raw data files
        """
        self.api_client = api_client or FootprintNetworkAPI()
        self.raw_data_path = raw_data_path
        
        # Ensure data directories exist
        self._ensure_directories()
        
        logger.info(f"Initialized FootprintDataStorage with data path: {raw_data_path}")
    
    def _ensure_directories(self):
        """Create the necessary directory structure if it doesn't exist."""
        # Main raw data directory
        os.makedirs(self.raw_data_path, exist_ok=True)
        
        # Subdirectories for different types of data
        for subdir in ['countries', 'years', 'types', 'data']:
            os.makedirs(os.path.join(self.raw_data_path, subdir), exist_ok=True)
            
        logger.info(f"Ensured directory structure exists at {self.raw_data_path}")
    
    def _save_json(self, data: Union[List[Any], Dict[str, Any]], filepath: str) -> None:
        """
        Save data as JSON to the specified filepath.
        
        Args:
            data: Data to save (list or dict)
            filepath: Path to save the JSON file
        """
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"Successfully saved data to {filepath}")
            
        except Exception as e:
            logger.error(f"Failed to save data to {filepath}: {str(e)}")
            raise
    
    def fetch_and_store_countries(self) -> str:
        """
        Fetch countries data from API and store locally.
        
        Returns:
            Path to the saved file
        """
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"countries_{timestamp}.json"
        filepath = os.path.join(self.raw_data_path, "countries", filename)
        
        # Fetch data from API
        logger.info("Fetching countries data from API")
        countries_data = self.api_client.get_countries()
        
        # Save data
        self._save_json(countries_data, filepath)
        
        return filepath
    
    def fetch_and_store_years(self) -> str:
        """
        Fetch years data from API and store locally.
        
        Returns:
            Path to the saved file
        """
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"years_{timestamp}.json"
        filepath = os.path.join(self.raw_data_path, "years", filename)
        
        # Fetch data from API
        logger.info("Fetching years data from API")
        years_data = self.api_client.get_years()
        
        # Save data
        self._save_json(years_data, filepath)
        
        return filepath
    
    def fetch_and_store_record_types(self) -> str:
        """
        Fetch record types data from API and store locally.
        
        Returns:
            Path to the saved file
        """
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"record_types_{timestamp}.json"
        filepath = os.path.join(self.raw_data_path, "types", filename)
        
        # Fetch data from API
        logger.info("Fetching record types data from API")
        types_data = self.api_client.get_types()
        
        # Save data
        self._save_json(types_data, filepath)
        
        return filepath
    
    def fetch_and_store_country_data(self, country_code: str, year: Optional[int] = None,
                                    record_types: Optional[List[str]] = None) -> str:
        """
        Fetch data for a specific country and store locally.
        
        Args:
            country_code: Country code to fetch data for
            year: Specific year to fetch, or None for all years
            record_types: List of record types to filter by, or None for all types
            
        Returns:
            Path to the saved file
        """
        # Generate filename with components and timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        year_part = f"_{year}" if year else "_all_years"
        types_part = f"_{'-'.join(record_types)}" if record_types else ""
        
        filename = f"country_{country_code}{year_part}{types_part}_{timestamp}.json"
        filepath = os.path.join(self.raw_data_path, "data", filename)
        
        # Fetch data from API
        logger.info(f"Fetching data for country {country_code}{' for year '+str(year) if year else ' for all years'}")
        
        if year and record_types:
            # Specific year and record types
            # API doesn't support multiple record types in a single call, so we'll make separate calls
            data = []
            for record_type in record_types:
                try:
                    record_data = self.api_client.get_data_for_record_type(country_code, year, record_type)
                    data.extend(record_data)
                    logger.info(f"Retrieved {len(record_data)} records for type {record_type}")
                except Exception as e:
                    logger.error(f"Error retrieving record type {record_type}: {str(e)}")
                    # Continue with other record types even if one fails
        elif year:
            # Specific year, all record types
            data = self.api_client.get_data_for_country_year(country_code, year)
        else:
            # All years
            data = self.api_client.get_data_for_country_year(country_code, 'all')
        
        # Save data
        self._save_json(data, filepath)
        
        return filepath
    
    def fetch_and_store_year_data(self, year: int) -> str:
        """
        Fetch data for all countries for a specific year and store locally.
        
        Args:
            year: Year to fetch data for
            
        Returns:
            Path to the saved file
        """
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"year_{year}_all_countries_{timestamp}.json"
        filepath = os.path.join(self.raw_data_path, "data", filename)
        
        # Fetch data from API
        logger.info(f"Fetching data for all countries for year {year}")
        data = self.api_client.get_data_for_country_year("all", year)
        
        # Save data
        self._save_json(data, filepath)
        
        return filepath
    
    def fetch_and_store_bulk_data(self, countries: List[str], years: List[int], 
                                  record_types: Optional[List[str]] = None) -> Dict[str, str]:
        """
        Fetch and store data for multiple countries and years.
        
        Args:
            countries: List of country codes to fetch
            years: List of years to fetch
            record_types: Optional list of record types to filter by
            
        Returns:
            Dictionary mapping country_code-year to saved file paths
        """
        results = {}
        
        for country_code in countries:
            for year in years:
                try:
                    filepath = self.fetch_and_store_country_data(
                        country_code=country_code,
                        year=year,
                        record_types=record_types
                    )
                    results[f"{country_code}-{year}"] = filepath
                    
                except Exception as e:
                    logger.error(f"Failed to fetch data for country {country_code}, year {year}: {str(e)}")
        
        return results
