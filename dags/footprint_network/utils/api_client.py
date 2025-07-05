"""
API client for interacting with the Global Footprint Network API.
"""

import time
import requests
import logging
from typing import Dict, List, Any, Optional, Union
import json
import sys
import os
from pathlib import Path
from requests.exceptions import RequestException, Timeout, HTTPError

# Add parent directory to Python path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.settings import API_BASE_URL, API_USERNAME, API_KEY
from utils.logging_utils import setup_logger

# Set up logger for this module
logger = setup_logger("api_client")

class FootprintNetworkAPI:
    """
    Client for interacting with the Global Footprint Network API.
    """
    
    def __init__(self, base_url: str = API_BASE_URL, username: str = API_USERNAME,
                 api_key: str = API_KEY, max_retries: int = 3, retry_delay: int = 2):
        """
        Initialize the API client.
        
        Args:
            base_url (str): The base URL for the API
            username (str): Username for basic authentication
            api_key (str): API key for authentication (used as password)
            max_retries (int): Maximum number of retries for failed requests
            retry_delay (int): Delay between retries in seconds
        """
        self.base_url = base_url
        self.username = username
        self.api_key = api_key
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        
        # Set default headers according to API documentation
        self.session.headers.update({
            'Content-Type': 'application/json',
            'HTTP_ACCEPT': 'application/json'
        })
        
        logger.info(f"Initialized API client for {base_url}")
    
    def _make_request(self, method: str, endpoint: str, 
                     params: Optional[Dict[str, Any]] = None, 
                     data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make an HTTP request to the API with retry logic.
        
        Args:
            method (str): HTTP method (GET, POST, etc.)
            endpoint (str): API endpoint to call
            params (dict, optional): Query parameters
            data (dict, optional): Request body for POST/PUT requests
            
        Returns:
            dict: JSON response from the API
            
        Raises:
            HTTPError: If the API request fails after all retries
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        # Initialize params dict if it doesn't exist
        if params is None:
            params = {}
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Making {method} request to {url} (Attempt {attempt + 1}/{self.max_retries})")
                
                # Use HTTP Basic Authentication with username and API key
                auth = (self.username, self.api_key)
                
                if method.upper() == 'GET':
                    response = self.session.get(url, params=params, auth=auth, timeout=30)
                elif method.upper() == 'POST':
                    response = self.session.post(url, params=params, json=data, auth=auth, timeout=30)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                # Raise an exception for 4XX/5XX status codes
                response.raise_for_status()
                
                return response.json()
                
            except (RequestException, Timeout) as e:
                logger.warning(f"Request failed (Attempt {attempt + 1}/{self.max_retries}): {str(e)}")
                
                # If this was the last attempt, raise the exception
                if attempt == self.max_retries - 1:
                    logger.error(f"Request failed after {self.max_retries} attempts: {str(e)}")
                    raise HTTPError(f"Request to {url} failed after {self.max_retries} attempts") from e
                
                # Wait before retrying
                time.sleep(self.retry_delay)
    
    def get_types(self) -> List[Dict[str, Any]]:
        """
        Get list of data types available from the API.
        
        Returns:
            list: List of available data types
        """
        logger.info("Fetching list of data types")
        try:
            response = self._make_request('GET', '/types')
            logger.info(f"Retrieved data types successfully")
            return response
        except Exception as e:
            logger.error(f"Failed to retrieve data types: {str(e)}")
            raise

    def get_countries(self) -> List[Dict[str, Any]]:
        """
        Get list of available countries from the API.
        
        Returns:
            list: List of country dictionaries with countryCode and countryName
        """
        logger.info("Fetching list of countries")
        try:
            response = self._make_request('GET', '/countries')
            logger.info(f"Retrieved countries successfully")
            return response
        except Exception as e:
            logger.error(f"Failed to retrieve countries: {str(e)}")
            raise
    
    def get_countries_count(self) -> int:
        """
        Get count of available countries from the API.
        
        Returns:
            int: Number of available countries
        """
        logger.info("Fetching count of available countries")
        try:
            response = self._make_request('GET', '/countries/count')
            logger.info(f"Retrieved countries count successfully")
            return response
        except Exception as e:
            logger.error(f"Failed to retrieve countries count: {str(e)}")
            raise
    
    def get_country_data(self, country_code: str) -> Dict[str, Any]:
        """
        Get data for a specific country by its country code.
        
        Args:
            country_code (str): Country code identifier
            
        Returns:
            dict: Country data
        """
        logger.info(f"Fetching data for country code {country_code}")
        
        try:
            # Based on documentation, we should use the country code directly
            endpoint = f"/countries/{country_code}"
            response = self._make_request('GET', endpoint)
            logger.info(f"Retrieved data for country {country_code}")
            return response
        except Exception as e:
            logger.error(f"Failed to retrieve data for country {country_code}: {str(e)}")
            raise
    
    def get_data_for_country_year(self, country_code: str, year: Union[int, str]) -> List[Dict[str, Any]]:
        """
        Get data for a specific country and year.
        
        Args:
            country_code (str): Country code identifier or 'all' for all countries
            year (int or str): Year to retrieve data for or 'all' for all years
            
        Returns:
            list: List of data records
        """
        logger.info(f"Fetching data for country {country_code} for year {year}")
        
        try:
            # Correctly formatted endpoint according to documentation
            endpoint = f"/data/{country_code}/{year}"
            response = self._make_request('GET', endpoint)
            logger.info(f"Retrieved data for country {country_code} for year {year}")
            return response
        except Exception as e:
            logger.error(f"Failed to retrieve data for country {country_code} for year {year}: {str(e)}")
            raise
    
    def get_data_for_record_type(self, country_code: str, year: Union[int, str], 
                               record_type: Union[str, List[str]]) -> List[Dict[str, Any]]:
        """
        Get data for specific country, year and record type(s).
        
        Args:
            country_code (str): Country code identifier or 'all' for all countries
            year (int or str): Year to retrieve data for or 'all' for all years
            record_type (str or list): Record type code(s) from types endpoint
            
        Returns:
            list: List of filtered data records
        """
        # Convert record type list to comma-separated string if needed
        if isinstance(record_type, list):
            record_type = ",".join(record_type)
            
        logger.info(f"Fetching data for country {country_code}, year {year}, record type(s) {record_type}")
        
        try:
            # Correctly formatted endpoint according to documentation
            endpoint = f"/data/{country_code}/{year}/{record_type}"
            response = self._make_request('GET', endpoint)
            logger.info(f"Retrieved data for country {country_code}, year {year}, record type(s) {record_type}")
            return response
        except Exception as e:
            logger.error(f"Failed to retrieve data for country {country_code}, year {year}, record type(s) {record_type}: {str(e)}")
            raise
            
    def get_years(self) -> List[Dict[str, Any]]:
        """
        Get list of available years from the API.
        
        Returns:
            list: List of years for which data is available
        """
        logger.info("Fetching list of available years")
        try:
            response = self._make_request('GET', '/years')
            logger.info(f"Retrieved years successfully")
            return response
        except Exception as e:
            logger.error(f"Failed to retrieve years: {str(e)}")
            raise
    
    def get_years_count(self) -> int:
        """
        Get count of available years.
        
        Returns:
            int: Count of available years
        """
        logger.info("Fetching count of available years")
        response = self._make_request('GET', '/years/count')
        logger.info("Retrieved years count successfully")
        return response
        
    def get_types(self) -> List[Dict[str, Any]]:
        """
        Get available record types for which data is available.
        
        Returns:
            list: List of available record types
        """
        logger.info("Fetching available record types")
        response = self._make_request('GET', '/types')
        logger.info(f"Retrieved {len(response)} record types successfully")
        return response
        
    def get_types_count(self) -> int:
        """
        Get count of available record types.
        
        Returns:
            int: Count of available record types
        """
        logger.info("Fetching count of available record types")
        response = self._make_request('GET', '/types/count')
        logger.info("Retrieved record types count successfully")
        return response
    
    def get_all_countries_data(self, year: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get data for all countries, optionally filtered by year.
        
        Args:
            year (int, optional): Specific year to retrieve data for
            
        Returns:
            list: List of country data dictionaries
        """
        logger.info(f"Fetching data for all countries{' for year ' + str(year) if year else ''}")
        
        try:
            # First get all countries
            countries = self.get_countries()
            
            all_countries_data = []
            for country in countries:
                country_code = country.get('countryCode')
                if country_code:
                    try:
                        country_data = self._make_request('GET', f"/countries/{country_code}")
                        all_countries_data.append(country_data)
                    except Exception as e:
                        logger.warning(f"Could not retrieve data for country {country_code}: {str(e)}")
            
            logger.info(f"Retrieved data for {len(all_countries_data)} countries")
            return all_countries_data
        except Exception as e:
            logger.error(f"Failed to retrieve data for all countries: {str(e)}")
            raise
