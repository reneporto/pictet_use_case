"""
Core data transformation module for the Global Footprint Network data.
This module implements Phase 1 of the transformation plan:
1. Create cleaned dimensional tables (countries, years, record types)
2. Build normalized fact table for ecological measures
3. Implement basic ecological indicators and time series transformations
4. Develop primary geographical aggregations
"""
import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from utils.data_transformer import FootprintDataTransformer

# Set up logging
logger = logging.getLogger(__name__)

# Mapping of regions by country code - this could be expanded or loaded from a file
# Just including some examples for demonstration
REGION_MAPPING = {
    # Europe
    '4': 'Europe',     # Albania
    '5': 'Europe',     # Andorra
    '40': 'Europe',    # Austria
    '203': 'Europe',   # Belgium
    '233': 'Europe',   # United Kingdom
    '232': 'Europe',   # Ukraine
    
    # North America
    '39': 'North America',    # USA
    '43': 'North America',    # Canada
    '142': 'North America',   # Mexico
    
    # Asia
    '35': 'Asia',     # China
    '113': 'Asia',    # India
    '114': 'Asia',    # Indonesia
    '116': 'Asia',    # Iran
    '117': 'Asia',    # Iraq
    '119': 'Asia',    # Japan
    
    # Africa
    '66': 'Africa',    # Ethiopia
    '83': 'Africa',    # Ghana
    '108': 'Africa',   # Kenya
    '133': 'Africa',   # Morocco
    '159': 'Africa',   # Nigeria
    '209': 'Africa',   # South Africa
    
    # South America
    '31': 'South America',    # Brazil
    '48': 'South America',    # Chile
    '49': 'South America',    # Colombia
    '63': 'South America',    # Ecuador
    '176': 'South America',   # Peru
    '238': 'South America',   # Venezuela
    
    # Oceania
    '14': 'Oceania',    # Australia
    '166': 'Oceania',   # New Zealand
    '174': 'Oceania',   # Papua New Guinea
}

# Income classifications based on World Bank groupings
# This is a simplified version and could be expanded or loaded from a file
INCOME_MAPPING = {
    # High income
    '14': 'High Income',    # Australia
    '40': 'High Income',    # Austria
    '119': 'High Income',   # Japan
    '232': 'High Income',   # United Kingdom
    '39': 'High Income',    # USA
    
    # Upper middle income
    '31': 'Upper Middle Income',    # Brazil
    '35': 'Upper Middle Income',    # China
    '49': 'Upper Middle Income',    # Colombia
    '133': 'Upper Middle Income',   # Morocco
    '176': 'Upper Middle Income',   # Peru
    
    # Lower middle income
    '63': 'Lower Middle Income',    # Ecuador
    '83': 'Lower Middle Income',    # Ghana
    '113': 'Lower Middle Income',   # India
    '114': 'Lower Middle Income',   # Indonesia
    '116': 'Lower Middle Income',   # Iran
    
    # Low income
    '66': 'Low Income',     # Ethiopia
    '108': 'Low Income',    # Kenya
    '159': 'Low Income',    # Nigeria
}

class FootprintCoreTransformer:
    """
    Class for implementing core transformations on the Global Footprint Network data.
    """
    
    def __init__(self, base_transformer: Optional[FootprintDataTransformer] = None):
        """
        Initialize the core transformer.
        
        Args:
            base_transformer: Optional existing data transformer to use.
                              If None, a new one will be created.
        """
        self.base_transformer = base_transformer or FootprintDataTransformer()
        self.processed_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'processed')
        self.transformed_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'transformed')
        
        # Create transformed directory if it doesn't exist
        os.makedirs(self.transformed_dir, exist_ok=True)
        
    def _save_dataframe(self, df: pd.DataFrame, name: str) -> str:
        """
        Save a DataFrame to the transformed directory.
        
        Args:
            df: DataFrame to save
            name: Base name for the file (without timestamp)
            
        Returns:
            Path to the saved file
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = os.path.join(self.transformed_dir, f"{name}_{timestamp}.parquet")
        df.to_parquet(output_path, index=False)
        logger.info(f"Saved transformed {name} data to {output_path}")
        return output_path

    def clean_countries(self) -> pd.DataFrame:
        """
        Clean and transform the countries dimension table.
        
        Returns:
            Cleaned countries DataFrame
        """
        # Load the countries data
        countries = self.base_transformer.transform_countries()
        if countries.empty:
            logger.warning("No country data to clean")
            return pd.DataFrame()
        
        logger.info(f"Cleaning countries dimension with {len(countries)} rows")
        
        # Remove duplicates
        countries = countries.drop_duplicates(subset=['country_code'])
        
        # Add region mapping
        countries['region'] = countries['country_code'].map(REGION_MAPPING)
        
        # Add income classification
        countries['income_group'] = countries['country_code'].map(INCOME_MAPPING)
        
        # Fill missing values with "Unknown"
        countries['region'] = countries['region'].fillna("Unknown")
        countries['income_group'] = countries['income_group'].fillna("Unknown")
        
        # Convert to categorical for efficiency
        countries['region'] = countries['region'].astype('category')
        countries['income_group'] = countries['income_group'].astype('category')
        countries['score'] = countries['score'].astype('category')
        
        # Add a timestamp for this transformation
        countries['transformed_at'] = datetime.now()
        
        # Save the transformed data
        self._save_dataframe(countries, "dim_countries")
        
        return countries
    
    def clean_years(self) -> pd.DataFrame:
        """
        Clean and transform the years dimension table.
        
        Returns:
            Cleaned years DataFrame
        """
        # Load the years data
        years = self.base_transformer.transform_years()
        if years.empty:
            logger.warning("No year data to clean")
            return pd.DataFrame()
        
        logger.info(f"Cleaning years dimension with {len(years)} rows")
        
        # Remove duplicates
        years = years.drop_duplicates(subset=['year'])
        
        # Sort by year
        years = years.sort_values('year', ascending=False)
        
        # Add decade column
        years['decade'] = (years['year'] // 10) * 10
        
        # Create date columns for start and end of year
        years['start_date'] = pd.to_datetime(years['year'].astype(str) + '-01-01')
        years['end_date'] = pd.to_datetime(years['year'].astype(str) + '-12-31')
        
        # Add a timestamp for this transformation
        years['transformed_at'] = datetime.now()
        
        # Save the transformed data
        self._save_dataframe(years, "dim_years")
        
        return years
    
    def clean_record_types(self) -> pd.DataFrame:
        """
        Clean and transform the record types dimension table.
        
        Returns:
            Cleaned record types DataFrame
        """
        # Load the record types data
        record_types = self.base_transformer.transform_record_types()
        if record_types.empty:
            logger.warning("No record type data to clean")
            return pd.DataFrame()
        
        logger.info(f"Cleaning record types dimension with {len(record_types)} rows")
        
        # Remove duplicates
        record_types = record_types.drop_duplicates(subset=['record'])
        
        # Create category groupings
        # Define a mapping for categories
        category_mapping = {
            'BCpc': 'Biocapacity',
            'BC': 'Biocapacity',
            'EFCpc': 'Ecological Footprint',
            'EFC': 'Ecological Footprint',
            'pop': 'Population',
            'Land': 'Land Use',
            'gdp': 'Economic'
        }
        
        # Extract prefix from the code to map to categories
        record_types['category'] = record_types['code'].str[:4].map(category_mapping)
        record_types['category'] = record_types['category'].fillna('Other')
        
        # Convert to categorical
        record_types['category'] = record_types['category'].astype('category')
        
        # Add a timestamp for this transformation
        record_types['transformed_at'] = datetime.now()
        
        # Save the transformed data
        self._save_dataframe(record_types, "dim_record_types")
        
        return record_types
    
    def clean_ecological_measures(self) -> pd.DataFrame:
        """
        Clean and normalize the ecological measures fact table.
        
        Returns:
            Cleaned ecological measures DataFrame
        """
        # Load the ecological measures data
        measures = self.base_transformer.transform_ecological_measures()
        if measures.empty:
            logger.warning("No ecological measures data to clean")
            return pd.DataFrame()
        
        logger.info(f"Cleaning ecological measures fact table with {len(measures)} rows")
        
        # Remove duplicates
        measures = measures.drop_duplicates(subset=['year', 'country_code', 'record'])
        
        # Rename columns for consistency
        if 'countryName' in measures.columns:
            measures = measures.rename(columns={'countryName': 'country_name_orig', 'shortName': 'short_name_orig', 'isoa2': 'iso_a2_orig'})
        
        # Handle missing values in the component columns
        numeric_cols = ['crop_land', 'grazing_land', 'forest_land', 'fishing_ground', 'builtup_land', 'carbon', 'value']
        for col in numeric_cols:
            if col in measures.columns:
                # Fill with 0 or other appropriate value
                measures[col] = measures[col].fillna(0)
        
        # Add a timestamp for this transformation
        measures['transformed_at'] = datetime.now()
        
        # Save the transformed data
        self._save_dataframe(measures, "fact_ecological_measures")
        
        return measures
    
    def calculate_ecological_indicators(self, measures: Optional[pd.DataFrame] = None, 
                                       countries: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Calculate basic ecological indicators from the measures data.
        
        Args:
            measures: Optional measures DataFrame. If None, it will be loaded.
            countries: Optional countries DataFrame. If None, it will be loaded.
            
        Returns:
            DataFrame with ecological indicators
        """
        # Load data if not provided
        if measures is None:
            measures = self.clean_ecological_measures()
        if countries is None:
            countries = self.clean_countries()
            
        if measures.empty or countries.empty:
            logger.warning("Missing data for calculating ecological indicators")
            return pd.DataFrame()
        
        logger.info("Calculating ecological indicators")
        
        # Create a working copy of the measures DataFrame
        indicators = measures.copy()
        
        # Filter for biocapacity and footprint records
        biocapacity = indicators[indicators['record'] == 'BiocapPerCap'].copy()
        footprint = indicators[indicators['record'] == 'EFConsPerCap'].copy()
        
        # Merge to calculate ecological deficit/reserve
        if not biocapacity.empty and not footprint.empty:
            # Prepare DataFrames for merging
            biocapacity = biocapacity.rename(columns={'value': 'biocapacity'})
            footprint = footprint.rename(columns={'value': 'footprint'})
            
            # Select relevant columns for merging
            biocap_df = biocapacity[['country_code', 'year', 'biocapacity']]
            footprint_df = footprint[['country_code', 'year', 'footprint']]
            
            # Merge the DataFrames
            eco_balance = pd.merge(
                biocap_df,
                footprint_df,
                on=['country_code', 'year'],
                how='outer'
            )
            
            # Calculate ecological balance
            eco_balance['ecological_balance'] = eco_balance['biocapacity'] - eco_balance['footprint']
            eco_balance['ecological_ratio'] = eco_balance['biocapacity'] / eco_balance['footprint']
            eco_balance['is_deficit'] = eco_balance['ecological_balance'] < 0
            
            # Add country information
            eco_balance = pd.merge(
                eco_balance,
                countries[['country_code', 'country_name', 'region', 'income_group']],
                on='country_code',
                how='left'
            )
            
            # Add a timestamp for this transformation
            eco_balance['transformed_at'] = datetime.now()
            
            # Save the transformed data
            self._save_dataframe(eco_balance, "indicator_ecological_balance")
            
            return eco_balance
        else:
            logger.warning("Missing biocapacity or footprint data for calculating indicators")
            return pd.DataFrame()
    
    def calculate_footprint_composition(self, measures: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Calculate the composition of ecological footprints.
        
        Args:
            measures: Optional measures DataFrame. If None, it will be loaded.
            
        Returns:
            DataFrame with footprint composition
        """
        # Load data if not provided
        if measures is None:
            measures = self.clean_ecological_measures()
            
        if measures.empty:
            logger.warning("Missing data for calculating footprint composition")
            return pd.DataFrame()
        
        logger.info("Calculating footprint composition")
        
        # Create a working copy of the measures DataFrame
        composition = measures.copy()
        
        # Filter for consumption footprint records
        footprint = composition[composition['record'] == 'EFConsPerCap'].copy()
        
        if not footprint.empty:
            # Calculate total for each component
            component_cols = ['crop_land', 'grazing_land', 'forest_land', 'fishing_ground', 'builtup_land', 'carbon']
            
            # Ensure all component columns exist
            for col in component_cols:
                if col not in footprint.columns:
                    footprint[col] = 0
            
            # Calculate the percentage of each component
            for col in component_cols:
                footprint[f'{col}_pct'] = footprint[col] / footprint['value'] * 100
            
            # Calculate carbon dependency ratio
            footprint['carbon_dependency'] = footprint['carbon'] / footprint['value'] * 100
            
            # Add a timestamp for this transformation
            footprint['transformed_at'] = datetime.now()
            
            # Save the transformed data
            self._save_dataframe(footprint, "indicator_footprint_composition")
            
            return footprint
        else:
            logger.warning("Missing footprint data for calculating composition")
            return pd.DataFrame()
    
    def calculate_time_series_changes(self, measures: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Calculate time series changes for key metrics.
        
        Args:
            measures: Optional measures DataFrame. If None, it will be loaded.
            
        Returns:
            DataFrame with time series changes
        """
        # Load data if not provided
        if measures is None:
            measures = self.clean_ecological_measures()
            
        if measures.empty:
            logger.warning("Missing data for calculating time series changes")
            return pd.DataFrame()
        
        logger.info("Calculating time series changes")
        
        # Filter for relevant records
        records_of_interest = ['BiocapPerCap', 'EFConsPerCap', 'Population', 'GDP']
        time_series = measures[measures['record'].isin(records_of_interest)].copy()
        
        if time_series.empty:
            logger.warning("No relevant records found for time series analysis")
            return pd.DataFrame()
        
        # Create a pivot table with years as columns
        pivot_df = time_series.pivot_table(
            index=['country_code', 'record'], 
            columns='year', 
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Sort columns for time series operations
        year_cols = sorted([col for col in pivot_df.columns if isinstance(col, int)])
        
        # Calculate year-over-year changes
        for i in range(1, len(year_cols)):
            curr_year = year_cols[i]
            prev_year = year_cols[i-1]
            pivot_df[f'change_{prev_year}_to_{curr_year}'] = pivot_df[curr_year] - pivot_df[prev_year]
            pivot_df[f'pct_change_{prev_year}_to_{curr_year}'] = (
                (pivot_df[curr_year] - pivot_df[prev_year]) / pivot_df[prev_year] * 100
            )
        
        # Calculate average annual change
        if len(year_cols) >= 2:
            first_year = year_cols[0]
            last_year = year_cols[-1]
            num_years = last_year - first_year
            
            if num_years > 0:
                pivot_df['avg_annual_change'] = (pivot_df[last_year] - pivot_df[first_year]) / num_years
                pivot_df['avg_annual_pct_change'] = (
                    ((pivot_df[last_year] / pivot_df[first_year]) ** (1 / num_years) - 1) * 100
                )
        
        # Restore the original structure with additional columns
        time_series_changes = pivot_df.melt(
            id_vars=['country_code', 'record'], 
            var_name='metric',
            value_name='value'
        )
        
        # Ensure metric column is stored as string
        time_series_changes['metric'] = time_series_changes['metric'].astype(str)
        
        # Add a timestamp for this transformation
        time_series_changes['transformed_at'] = datetime.now()
        
        # Save the transformed data
        self._save_dataframe(time_series_changes, "indicator_time_series_changes")
        
        return time_series_changes
    
    def create_geographical_aggregations(self, measures: Optional[pd.DataFrame] = None, 
                                        countries: Optional[pd.DataFrame] = None) -> Dict[str, pd.DataFrame]:
        """
        Create geographical aggregations of measures data.
        
        Args:
            measures: Optional measures DataFrame. If None, it will be loaded.
            countries: Optional countries DataFrame. If None, it will be loaded.
            
        Returns:
            Dictionary with geographical aggregations DataFrames
        """
        # Load data if not provided
        if measures is None:
            measures = self.clean_ecological_measures()
        if countries is None:
            countries = self.clean_countries()
            
        if measures.empty or countries.empty:
            logger.warning("Missing data for geographical aggregations")
            return {}
        
        logger.info("Creating geographical aggregations")
        
        # Merge measures with countries to get region and income group
        geo_data = pd.merge(
            measures,
            countries[['country_code', 'region', 'income_group']],
            on='country_code',
            how='left'
        )
        
        # Filter for most common record types
        common_records = ['BiocapPerCap', 'EFConsPerCap', 'Population', 'GDP']
        geo_data = geo_data[geo_data['record'].isin(common_records)]
        
        # Create region-level aggregations
        # Only include columns that exist in the data
        agg_columns = {'value': ['mean', 'median', 'std', 'min', 'max', 'count']}
        
        # Check if other columns exist before adding to aggregation
        if 'population' in geo_data.columns:
            agg_columns['population'] = 'sum'
        if 'gdp' in geo_data.columns:
            agg_columns['gdp'] = 'sum'
            
        region_agg = geo_data.groupby(['region', 'year', 'record'], observed=False).agg(agg_columns).reset_index()
        
        # Fix column names after aggregation
        region_agg.columns = ['_'.join(col).strip('_') for col in region_agg.columns.values]
        
        # Create income group aggregations
        # Use the same agg_columns dictionary to ensure consistency
        income_agg = geo_data.groupby(['income_group', 'year', 'record'], observed=False).agg(agg_columns).reset_index()
        
        # Fix column names after aggregation
        income_agg.columns = ['_'.join(col).strip('_') for col in income_agg.columns.values]
        
        # Calculate population-weighted metrics for regions only if Population record exists
        population_records = geo_data[geo_data['record'] == 'Population']
        
        # Only proceed with population weighting if we have population data
        if not population_records.empty:
            # Create a population lookup from the Population record type
            pop_lookup = population_records[['country_code', 'year', 'value']].rename(columns={'value': 'population'})
            
            # Merge population data with other metrics
            weighted_metrics = pd.merge(
                geo_data[geo_data['record'] != 'Population'],
                pop_lookup,
                on=['country_code', 'year'],
                how='inner'
            )
            
            # Calculate weighted values
            weighted_metrics['weighted_value'] = weighted_metrics['value'] * weighted_metrics['population']
            
            # Calculate population-weighted metrics by region
            weighted_region_agg = weighted_metrics.groupby(['region', 'year', 'record'], observed=False).agg({
                'weighted_value': 'sum',
                'population': 'sum'
            }).reset_index()
            
            # Calculate population-weighted average
            weighted_region_agg['population_weighted_avg'] = weighted_region_agg['weighted_value'] / weighted_region_agg['population']
        else:
            weighted_region_agg = pd.DataFrame()
        
        # Add timestamps
        region_agg['transformed_at'] = datetime.now()
        income_agg['transformed_at'] = datetime.now()
        if not weighted_region_agg.empty:
            weighted_region_agg['transformed_at'] = datetime.now()
        
        # Save the transformed data
        self._save_dataframe(region_agg, "agg_by_region")
        self._save_dataframe(income_agg, "agg_by_income")
        if not weighted_region_agg.empty:
            self._save_dataframe(weighted_region_agg, "agg_population_weighted")
        
        return {
            'region_aggregations': region_agg,
            'income_aggregations': income_agg,
            'weighted_aggregations': weighted_region_agg
        }
    
    def run_all_core_transformations(self) -> Dict[str, pd.DataFrame]:
        """
        Run all core transformations in the correct sequence.
        
        Returns:
            Dictionary with all transformed DataFrames
        """
        logger.info("Running all core transformations")
        
        # 1. Clean dimension tables
        countries = self.clean_countries()
        years = self.clean_years()
        record_types = self.clean_record_types()
        
        # 2. Clean fact table
        measures = self.clean_ecological_measures()
        
        # 3. Calculate indicators
        ecological_balance = self.calculate_ecological_indicators(measures, countries)
        footprint_composition = self.calculate_footprint_composition(measures)
        time_series_changes = self.calculate_time_series_changes(measures)
        
        # 4. Create geographical aggregations
        geo_aggs = self.create_geographical_aggregations(measures, countries)
        
        # Return all results
        results = {
            'dim_countries': countries,
            'dim_years': years,
            'dim_record_types': record_types,
            'fact_ecological_measures': measures,
            'indicator_ecological_balance': ecological_balance,
            'indicator_footprint_composition': footprint_composition,
            'indicator_time_series_changes': time_series_changes
        }
        
        # Add geographical aggregations
        results.update(geo_aggs)
        
        return results
