"""
Module for transforming raw JSON data from the Global Footprint Network API into pandas DataFrames.
"""
import os
import json
import glob
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd

# Set up logging
logger = logging.getLogger(__name__)

class FootprintDataTransformer:
    """
    Class for transforming footprint network data from JSON files into pandas DataFrames.
    """
    
    def __init__(self):
        """Initialize the data transformer."""
        self.base_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'raw')
        self.processed_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'processed')
        
        # Create processed directory if it doesn't exist
        os.makedirs(self.processed_dir, exist_ok=True)
        
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
    
    def transform_countries(self, filepath: Optional[str] = None) -> pd.DataFrame:
        """
        Transform country data from a JSON file into a DataFrame.
        
        Args:
            filepath: Path to the JSON file. If None, the most recent file is used.
            
        Returns:
            DataFrame with country data
        """
        # Get the filepath if not provided
        if filepath is None:
            countries_dir = os.path.join(self.base_dir, 'countries')
            filepath = self._get_latest_file(countries_dir)
            if not filepath:
                logger.warning("No country data files found")
                return pd.DataFrame()
        
        logger.info(f"Transforming country data from {filepath}")
        countries = self._read_json_file(filepath)
        
        # Create DataFrame
        df = pd.DataFrame(countries)
        
        # Rename columns to match our schema
        column_mapping = {
            'countryCode': 'country_code',
            'countryName': 'country_name',
            'shortName': 'short_name',
            'isoa2': 'iso_a2'
        }
        df = df.rename(columns=column_mapping)
        
        # Ensure consistent data types
        if 'country_code' in df.columns:
            df['country_code'] = df['country_code'].astype(str)
        
        # Ensure required columns are present
        required_columns = ['country_code', 'country_name']
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"Required column {col} not found in countries data")
                return pd.DataFrame()
        
        # Add timestamp column
        df['processed_at'] = datetime.now()
        
        # Save processed data
        output_path = os.path.join(self.processed_dir, f"countries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
        df.to_parquet(output_path, index=False)
        logger.info(f"Saved processed country data to {output_path}")
        
        return df
    
    def transform_years(self, filepath: Optional[str] = None) -> pd.DataFrame:
        """
        Transform year data from a JSON file into a DataFrame.
        
        Args:
            filepath: Path to the JSON file. If None, the most recent file is used.
            
        Returns:
            DataFrame with year data
        """
        # Get the filepath if not provided
        if filepath is None:
            years_dir = os.path.join(self.base_dir, 'years')
            filepath = self._get_latest_file(years_dir)
            if not filepath:
                logger.warning("No year data files found")
                return pd.DataFrame()
        
        logger.info(f"Transforming year data from {filepath}")
        years = self._read_json_file(filepath)
        
        # Create DataFrame
        df = pd.DataFrame(years)
        
        # Add timestamp column
        df['processed_at'] = datetime.now()
        
        # Save processed data
        output_path = os.path.join(self.processed_dir, f"years_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
        df.to_parquet(output_path, index=False)
        logger.info(f"Saved processed year data to {output_path}")
        
        return df
    
    def transform_record_types(self, filepath: Optional[str] = None) -> pd.DataFrame:
        """
        Transform record type data from a JSON file into a DataFrame.
        
        Args:
            filepath: Path to the JSON file. If None, the most recent file is used.
            
        Returns:
            DataFrame with record type data
        """
        # Get the filepath if not provided
        if filepath is None:
            types_dir = os.path.join(self.base_dir, 'types')
            filepath = self._get_latest_file(types_dir)
            if not filepath:
                logger.warning("No record type data files found")
                return pd.DataFrame()
        
        logger.info(f"Transforming record type data from {filepath}")
        record_types = self._read_json_file(filepath)
        
        # Create DataFrame
        df = pd.DataFrame(record_types)
        
        # Add timestamp column
        df['processed_at'] = datetime.now()
        
        # Save processed data
        output_path = os.path.join(self.processed_dir, f"record_types_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
        df.to_parquet(output_path, index=False)
        logger.info(f"Saved processed record type data to {output_path}")
        
        return df
    
    def transform_ecological_measures(self, country_code: Optional[str] = None, year: Optional[int] = None) -> pd.DataFrame:
        """
        Transform ecological measures data from JSON files into a DataFrame.
        
        Args:
            country_code: Specific country code to transform data for. If None, all countries are transformed.
            year: Specific year to transform data for. If None, all years are transformed.
            
        Returns:
            DataFrame with ecological measures data
        """
        data_dir = os.path.join(self.base_dir, 'data')
        if not os.path.exists(data_dir):
            logger.warning("No data directory found")
            return pd.DataFrame()
        
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
            return pd.DataFrame()
        
        # Sort files by modification time, newest first
        files.sort(key=os.path.getmtime, reverse=True)
        
        all_data = []
        for filepath in files:
            logger.info(f"Transforming ecological measures from {filepath}")
            try:
                measures = self._read_json_file(filepath)
                all_data.extend(measures)
            except Exception as e:
                logger.error(f"Error loading measures from {filepath}: {str(e)}")
        
        if not all_data:
            logger.warning("No ecological measures data loaded")
            return pd.DataFrame()
        
        # Create DataFrame
        df = pd.DataFrame(all_data)
        
        # Rename columns to match our schema
        column_mapping = {
            'countryCode': 'country_code',
            'cropLand': 'crop_land',
            'grazingLand': 'grazing_land',
            'forestLand': 'forest_land',
            'fishingGround': 'fishing_ground',
            'builtupLand': 'builtup_land'
        }
        df = df.rename(columns=column_mapping)
        
        # Ensure consistent data types
        if 'country_code' in df.columns:
            df['country_code'] = df['country_code'].astype(str)
        if 'year' in df.columns:
            df['year'] = df['year'].astype(int)
        
        # Add timestamp column
        df['processed_at'] = datetime.now()
        
        # Save processed data
        filename = f"ecological_measures"
        if country_code:
            filename += f"_{country_code}"
        if year:
            filename += f"_{year}"
        
        output_path = os.path.join(self.processed_dir, f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
        df.to_parquet(output_path, index=False)
        logger.info(f"Saved processed ecological measures data to {output_path}")
        
        return df
    
    def transform_all_data(self) -> Dict[str, pd.DataFrame]:
        """
        Transform all available data into DataFrames.
        
        Returns:
            Dictionary with DataFrames by data type
        """
        # Transform the data
        countries_df = self.transform_countries()
        years_df = self.transform_years()
        record_types_df = self.transform_record_types()
        measures_df = self.transform_ecological_measures()
        
        return {
            'countries': countries_df,
            'years': years_df,
            'record_types': record_types_df,
            'ecological_measures': measures_df
        }
        
    def create_analytics_view(self, dfs: Optional[Dict[str, pd.DataFrame]] = None) -> pd.DataFrame:
        """
        Create an analytics view by joining the different DataFrames.
        
        Args:
            dfs: Dictionary with DataFrames by data type. If None, they will be loaded.
            
        Returns:
            DataFrame with joined data for analytics
        """
        if dfs is None or any(df.empty for df in dfs.values()):
            dfs = self.transform_all_data()
            
        countries_df = dfs['countries'].copy()
        record_types_df = dfs['record_types'].copy()
        measures_df = dfs['ecological_measures'].copy()
        
        if countries_df.empty or record_types_df.empty or measures_df.empty:
            logger.warning("One or more required DataFrames are empty")
            return pd.DataFrame()
        
        # Ensure consistent data types for join keys
        countries_df['country_code'] = countries_df['country_code'].astype(str)
        measures_df['country_code'] = measures_df['country_code'].astype(str)
        
        # Log some information about the data types
        logger.info(f"Countries DataFrame country_code dtype: {countries_df['country_code'].dtype}")
        logger.info(f"Measures DataFrame country_code dtype: {measures_df['country_code'].dtype}")
        logger.info(f"Sample country codes from countries: {countries_df['country_code'].head(3).tolist()}")
        logger.info(f"Sample country codes from measures: {measures_df['country_code'].head(3).tolist()}")
        
        # Join countries with measures
        merged_df = pd.merge(
            measures_df,
            countries_df[['country_code', 'country_name', 'iso_a2']],
            on='country_code',
            how='left'
        )
        
        # Join record types
        merged_df = pd.merge(
            merged_df,
            record_types_df[['record', 'name', 'code']],
            on='record',
            how='left'
        )
        
        # Save the analytics view
        output_path = os.path.join(self.processed_dir, f"analytics_view_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
        merged_df.to_parquet(output_path, index=False)
        logger.info(f"Saved analytics view to {output_path}")
        
        return merged_df
