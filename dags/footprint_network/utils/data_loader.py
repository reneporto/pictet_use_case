"""
Data loader module for importing JSON data into DuckDB.
"""
import os
import json
import glob
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from .db_manager import FootprintDuckDBManager

# Set up logging
logger = logging.getLogger(__name__)

class FootprintDataLoader:
    """
    Class for loading footprint network data from JSON files into DuckDB.
    """
    
    def __init__(self, db_manager: Optional[FootprintDuckDBManager] = None):
        """
        Initialize the data loader.
        
        Args:
            db_manager: An instance of FootprintDuckDBManager. If None, a new one is created.
        """
        self.db_manager = db_manager if db_manager is not None else FootprintDuckDBManager()
        self.base_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'raw')
        
    def _read_json_file(self, filepath: str) -> List[Dict[str, Any]]:
        """
        Read a JSON file and return its contents.
        
        Args:
            filepath: Path to the JSON file
            
        Returns:
            The contents of the JSON file as a Python object
        """
        try:
            with open(filepath, 'r') as file:
                data = json.load(file)
            return data
        except Exception as e:
            logger.error(f"Error reading JSON file {filepath}: {str(e)}")
            raise
    
    def _get_latest_file(self, directory: str, pattern: str = "*.json") -> Optional[str]:
        """
        Get the most recently created JSON file in a directory.
        
        Args:
            directory: Directory to search in
            pattern: Glob pattern for files
            
        Returns:
            Path to the most recent file, or None if no files found
        """
        files = glob.glob(os.path.join(directory, pattern))
        if not files:
            return None
            
        # Sort by modification time, newest first
        latest = max(files, key=os.path.getmtime)
        return latest
    
    def load_countries(self, filepath: Optional[str] = None) -> int:
        """
        Load country data from a JSON file into the database.
        
        Args:
            filepath: Path to the JSON file. If None, the most recent file is used.
            
        Returns:
            Number of countries loaded
        """
        # Get the filepath if not provided
        if filepath is None:
            countries_dir = os.path.join(self.base_dir, 'countries')
            filepath = self._get_latest_file(countries_dir)
            if not filepath:
                logger.warning("No country data files found")
                return 0
        
        logger.info(f"Loading country data from {filepath}")
        countries = self._read_json_file(filepath)
        
        count = 0
        for country in countries:
            try:
                # Extract the required fields
                country_code = country.get('countryCode')
                country_name = country.get('countryName')
                short_name = country.get('shortName', country_name)
                iso_a2 = country.get('isoa2')
                
                if country_code is None or country_name is None:
                    logger.warning(f"Skipping country with missing required fields: {country}")
                    continue
                
                # Insert into the database
                self.db_manager.execute_query(
                    "INSERT INTO countries (country_code, country_name, short_name, iso_a2) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    (country_code, country_name, short_name, iso_a2)
                )
                count += 1
            except Exception as e:
                logger.error(f"Error loading country: {str(e)}")
        
        logger.info(f"Loaded {count} countries into the database")
        return count
    
    def load_years(self, filepath: Optional[str] = None) -> int:
        """
        Load year data from a JSON file into the database.
        
        Args:
            filepath: Path to the JSON file. If None, the most recent file is used.
            
        Returns:
            Number of years loaded
        """
        # Get the filepath if not provided
        if filepath is None:
            years_dir = os.path.join(self.base_dir, 'years')
            filepath = self._get_latest_file(years_dir)
            if not filepath:
                logger.warning("No year data files found")
                return 0
        
        logger.info(f"Loading year data from {filepath}")
        years = self._read_json_file(filepath)
        
        count = 0
        for year_obj in years:
            try:
                year = year_obj.get('year')
                if year is None:
                    logger.warning(f"Skipping year with missing required fields: {year_obj}")
                    continue
                
                # Insert into the database
                self.db_manager.execute_query(
                    "INSERT INTO years (year) VALUES (?) ON CONFLICT DO NOTHING",
                    (year,)
                )
                count += 1
            except Exception as e:
                logger.error(f"Error loading year: {str(e)}")
        
        logger.info(f"Loaded {count} years into the database")
        return count
    
    def load_record_types(self, filepath: Optional[str] = None) -> int:
        """
        Load record type data from a JSON file into the database.
        
        Args:
            filepath: Path to the JSON file. If None, the most recent file is used.
            
        Returns:
            Number of record types loaded
        """
        # Get the filepath if not provided
        if filepath is None:
            types_dir = os.path.join(self.base_dir, 'types')
            filepath = self._get_latest_file(types_dir)
            if not filepath:
                logger.warning("No record type data files found")
                return 0
        
        logger.info(f"Loading record type data from {filepath}")
        record_types = self._read_json_file(filepath)
        
        count = 0
        for record_type in record_types:
            try:
                # Extract the required fields
                code = record_type.get('code')
                name = record_type.get('name')
                note = record_type.get('note')
                record = record_type.get('record')
                
                if code is None or name is None:
                    logger.warning(f"Skipping record type with missing required fields: {record_type}")
                    continue
                
                # Insert into the database
                self.db_manager.execute_query(
                    "INSERT INTO record_types (code, name, note, record) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    (code, name, note, record)
                )
                count += 1
            except Exception as e:
                logger.error(f"Error loading record type: {str(e)}")
        
        logger.info(f"Loaded {count} record types into the database")
        return count
    
    def load_ecological_measures(self, country_code: Optional[str] = None, year: Optional[int] = None) -> int:
        """
        Load ecological measures data from JSON files into the database.
        
        Args:
            country_code: Specific country code to load data for. If None, all countries are loaded.
            year: Specific year to load data for. If None, all years are loaded.
            
        Returns:
            Number of measures loaded
        """
        data_dir = os.path.join(self.base_dir, 'data')
        if not os.path.exists(data_dir):
            logger.warning("No data directory found")
            return 0
        
        # Build the pattern based on filters
        if country_code and year:
            pattern = f"country_{country_code}_{year}_*.json"
        elif country_code:
            pattern = f"country_{country_code}_*.json"
        elif year:
            pattern = f"*_{year}_*.json"
        else:
            pattern = "*.json"
        
        files = glob.glob(os.path.join(data_dir, pattern))
        if not files:
            logger.warning(f"No data files found matching pattern: {pattern}")
            return 0
        
        # Sort files by modification time, newest first
        files.sort(key=os.path.getmtime, reverse=True)
        
        total_count = 0
        for filepath in files:
            logger.info(f"Loading ecological measures from {filepath}")
            try:
                measures = self._read_json_file(filepath)
                count = self._load_measures_batch(measures)
                total_count += count
                logger.info(f"Loaded {count} measures from {filepath}")
            except Exception as e:
                logger.error(f"Error loading measures from {filepath}: {str(e)}")
        
        logger.info(f"Loaded a total of {total_count} ecological measures into the database")
        return total_count
    
    def _get_record_code_mapping(self) -> Dict[str, str]:
        """
        Get a mapping of record names to record codes from the database.
        
        Returns:
            Dictionary mapping record names to record codes
        """
        mapping = {}
        try:
            # Query the database for record types
            result = self.db_manager.execute_query(
                "SELECT code, record FROM record_types"
            ).fetchall()
            
            # Build the mapping
            for row in result:
                code = row[0]
                record = row[1]
                mapping[record] = code
                
            logger.info(f"Loaded record code mapping with {len(mapping)} entries")
        except Exception as e:
            logger.error(f"Error getting record code mapping: {str(e)}")
        
        return mapping
    
    def _load_measures_batch(self, measures: List[Dict[str, Any]]) -> int:
        """
        Load a batch of ecological measures into the database.
        
        Args:
            measures: List of measure data
            
        Returns:
            Number of measures loaded
        """
        count = 0
        loaded_at = datetime.now()
        
        # Get mapping of record names to codes
        record_mapping = self._get_record_code_mapping()
        
        # If we have no mapping, let's try a direct approach with a comprehensive fallback mapping
        if not record_mapping:
            logger.warning("No record mapping found in database, using fallback mapping")
            record_mapping = {
                # Biocapacity
                "BiocapPerCap": "bcpc",
                "BiocapTotGHA": "bctot",
                
                # Ecological Footprint - Consumption
                "EFConsPerCap": "efcpc",
                "EFConsTotGHA": "efctot",
                
                # Ecological Footprint - Production
                "EFProdPerCap": "efppc", 
                "EFProdTotGHA": "efptot",
                
                # Ecological Footprint - Imports
                "EFImportsPerCap": "efipc",
                "EFImportsTotGHA": "efitot",
                
                # Ecological Footprint - Exports
                "EFExportsPerCap": "efepc",
                "EFExportsTotGHA": "efetot",
                
                # Area
                "AreaPerCap": "apc",
                "AreaTotHA": "atot",
                
                # Other metrics
                "Population": "pop",
                "Earths": "earth",
                "GDP-PPP": "gdpp", 
                "GDP-USD": "gdpus",
                "GDP": "gdp",  # Added based on the record_types JSON
                "HDI": "hdi",
                
                # Add any additional mappings if needed
            }
        
        for measure in measures:
            try:
                # Extract required fields
                country_code = measure.get('countryCode')
                year = measure.get('year')
                record_name = measure.get('record')  # This is the full record name
                
                if country_code is None or year is None or record_name is None:
                    logger.warning(f"Skipping measure with missing required fields: {measure}")
                    continue
                
                # Map the record name to the code
                record_code = record_mapping.get(record_name)
                if record_code is None:
                    logger.warning(f"No mapping found for record name: {record_name}, skipping")
                    continue
                
                # Extract the measure values
                crop_land = measure.get('cropLand')
                grazing_land = measure.get('grazingLand')
                forest_land = measure.get('forestLand')
                fishing_ground = measure.get('fishingGround')
                builtup_land = measure.get('builtupLand')
                carbon = measure.get('carbon')
                value = measure.get('value')
                score = measure.get('score')
                
                # Insert into the database
                self.db_manager.execute_query(
                    """
                    INSERT INTO ecological_measures 
                    (country_code, year, record, crop_land, grazing_land, forest_land, 
                     fishing_ground, builtup_land, carbon, value, score, loaded_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (country_code, year, record) DO UPDATE SET
                    crop_land = excluded.crop_land,
                    grazing_land = excluded.grazing_land,
                    forest_land = excluded.forest_land,
                    fishing_ground = excluded.fishing_ground,
                    builtup_land = excluded.builtup_land,
                    carbon = excluded.carbon,
                    value = excluded.value,
                    score = excluded.score,
                    loaded_at = excluded.loaded_at
                    """,
                    (country_code, year, record_code, crop_land, grazing_land, forest_land, 
                     fishing_ground, builtup_land, carbon, value, score, loaded_at)
                )
                count += 1
            except Exception as e:
                logger.error(f"Error loading measure: {str(e)}")
        
        return count
    
    def load_all_data(self) -> Dict[str, int]:
        """
        Load all available data into the database.
        
        Returns:
            Dictionary with counts of loaded items by type
        """
        # Ensure tables exist
        self.db_manager.create_tables()
        
        # Load the data
        countries_count = self.load_countries()
        years_count = self.load_years()
        record_types_count = self.load_record_types()
        measures_count = self.load_ecological_measures()
        
        return {
            'countries': countries_count,
            'years': years_count,
            'record_types': record_types_count,
            'ecological_measures': measures_count
        }
