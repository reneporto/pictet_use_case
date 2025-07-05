"""
Test script for validating the core data transformations.
"""
import os
import sys
import logging
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Add the parent directory to the path to import the utils
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.data_transformer_core import FootprintCoreTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger('test_core_transformations')

# Create plots directory if it doesn't exist
plots_dir = os.path.join(parent_dir, 'data', 'plots')
os.makedirs(plots_dir, exist_ok=True)

def test_dimension_tables():
    """
    Test the cleaning and transformation of dimension tables.
    """
    logger.info("Testing dimension table transformations...")
    transformer = FootprintCoreTransformer()
    
    # Clean dimension tables
    countries = transformer.clean_countries()
    years = transformer.clean_years()
    record_types = transformer.clean_record_types()
    
    # Log results
    if not countries.empty:
        logger.info(f"Transformed countries dimension: {len(countries)} rows")
        logger.info(f"Countries by region:\n{countries['region'].value_counts()}")
        logger.info(f"Countries by income group:\n{countries['income_group'].value_counts()}")
    
    if not years.empty:
        logger.info(f"Transformed years dimension: {len(years)} rows")
        logger.info(f"Year range: {years['year'].min()} to {years['year'].max()}")
        logger.info(f"Decades:\n{years['decade'].value_counts().sort_index()}")
    
    if not record_types.empty:
        logger.info(f"Transformed record types dimension: {len(record_types)} rows")
        logger.info(f"Record types by category:\n{record_types['category'].value_counts()}")
    
    return {
        'countries': countries,
        'years': years,
        'record_types': record_types
    }

def test_fact_table():
    """
    Test the cleaning and transformation of the ecological measures fact table.
    """
    logger.info("Testing fact table transformation...")
    transformer = FootprintCoreTransformer()
    
    # Clean fact table
    measures = transformer.clean_ecological_measures()
    
    # Log results
    if not measures.empty:
        logger.info(f"Transformed ecological measures fact table: {len(measures)} rows")
        logger.info(f"Records by type:\n{measures['record'].value_counts().head(10)}")
        logger.info(f"Records by year:\n{measures['year'].value_counts().sort_index().head(10)}")
        
        # Check for missing values
        null_counts = measures[['crop_land', 'grazing_land', 'forest_land', 
                               'fishing_ground', 'builtup_land', 'carbon', 'value']].isnull().sum()
        logger.info(f"Missing values after cleaning:\n{null_counts}")
    
    return measures

def test_ecological_indicators():
    """
    Test the calculation of ecological indicators.
    """
    logger.info("Testing ecological indicator calculations...")
    transformer = FootprintCoreTransformer()
    
    # Calculate indicators
    ecological_balance = transformer.calculate_ecological_indicators()
    footprint_composition = transformer.calculate_footprint_composition()
    
    # Log results
    if not ecological_balance.empty:
        logger.info(f"Ecological balance indicators: {len(ecological_balance)} rows")
        
        # Find countries with largest ecological reserve (positive balance)
        top_reserve = ecological_balance.sort_values('ecological_balance', ascending=False).head(5)
        logger.info("Top 5 countries with largest ecological reserve:")
        for _, row in top_reserve.iterrows():
            logger.info(f"  {row['country_name']}: {row['ecological_balance']:.2f} gha/person")
        
        # Find countries with largest ecological deficit (negative balance)
        top_deficit = ecological_balance.sort_values('ecological_balance').head(5)
        logger.info("Top 5 countries with largest ecological deficit:")
        for _, row in top_deficit.iterrows():
            logger.info(f"  {row['country_name']}: {row['ecological_balance']:.2f} gha/person")
        
        # Create a plot of ecological balance by region
        try:
            plt.figure(figsize=(10, 6))
            sns.boxplot(x='region', y='ecological_balance', data=ecological_balance)
            plt.title('Ecological Balance by Region')
            plt.xlabel('Region')
            plt.ylabel('Ecological Balance (gha/person)')
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            # Save the plot
            plot_path = os.path.join(plots_dir, f"ecological_balance_by_region_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
            plt.savefig(plot_path)
            logger.info(f"Saved ecological balance plot to {plot_path}")
        except Exception as e:
            logger.warning(f"Could not create ecological balance plot: {str(e)}")
    
    if not footprint_composition.empty:
        logger.info(f"Footprint composition indicators: {len(footprint_composition)} rows")
        
        # Calculate average component percentages
        avg_components = {}
        for col in ['carbon_pct', 'crop_land_pct', 'grazing_land_pct', 'forest_land_pct', 
                    'fishing_ground_pct', 'builtup_land_pct']:
            if col in footprint_composition.columns:
                avg_components[col] = footprint_composition[col].mean()
        
        logger.info("Average footprint composition:")
        for comp, pct in avg_components.items():
            logger.info(f"  {comp}: {pct:.2f}%")
        
        # Create a plot of carbon dependency by income group
        try:
            # First merge with countries to get income group
            countries = transformer.clean_countries()
            if not countries.empty:
                comp_with_income = pd.merge(
                    footprint_composition,
                    countries[['country_code', 'income_group']],
                    on='country_code',
                    how='left'
                )
                
                plt.figure(figsize=(10, 6))
                sns.boxplot(x='income_group', y='carbon_dependency', data=comp_with_income)
                plt.title('Carbon Dependency by Income Group')
                plt.xlabel('Income Group')
                plt.ylabel('Carbon Dependency (%)')
                plt.xticks(rotation=45)
                plt.tight_layout()
                
                # Save the plot
                plot_path = os.path.join(plots_dir, f"carbon_dependency_by_income_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                plt.savefig(plot_path)
                logger.info(f"Saved carbon dependency plot to {plot_path}")
        except Exception as e:
            logger.warning(f"Could not create carbon dependency plot: {str(e)}")
    
    return {
        'ecological_balance': ecological_balance,
        'footprint_composition': footprint_composition
    }

def test_time_series():
    """
    Test the calculation of time series changes.
    """
    logger.info("Testing time series transformations...")
    transformer = FootprintCoreTransformer()
    
    # Calculate time series changes
    time_series = transformer.calculate_time_series_changes()
    
    # Log results
    if not time_series.empty:
        logger.info(f"Time series indicators: {len(time_series)} rows")
        
        # Find metrics with largest changes
        change_metrics = [col for col in time_series['metric'].unique() if 'change' in str(col)]
        if change_metrics:
            for metric in change_metrics[:3]:  # Look at first few change metrics
                top_changes = time_series[time_series['metric'] == metric].sort_values('value', ascending=False).head(5)
                if not top_changes.empty:
                    logger.info(f"Top 5 largest {metric}:")
                    for _, row in top_changes.iterrows():
                        logger.info(f"  Country Code: {row['country_code']}, Record: {row['record']}, Value: {row['value']:.2f}")
    
    return time_series

def test_geographical_aggregations():
    """
    Test the creation of geographical aggregations.
    """
    logger.info("Testing geographical aggregations...")
    transformer = FootprintCoreTransformer()
    
    # Create geographical aggregations
    geo_aggs = transformer.create_geographical_aggregations()
    
    # Log results
    if geo_aggs:
        for agg_name, agg_df in geo_aggs.items():
            if not agg_df.empty:
                logger.info(f"{agg_name}: {len(agg_df)} rows")
                
                # For region aggregations, show some key metrics
                if agg_name == 'region_aggregations':
                    # Check if we have BiocapPerCap and the most recent year
                    recent_years = sorted(agg_df['year'].unique(), reverse=True)
                    if recent_years and 'BiocapPerCap' in agg_df['record'].unique():
                        recent_biocap = agg_df[(agg_df['year'] == recent_years[0]) & 
                                              (agg_df['record'] == 'BiocapPerCap')]
                        
                        if not recent_biocap.empty:
                            logger.info(f"Biocapacity per person by region ({recent_years[0]}):")
                            for _, row in recent_biocap.iterrows():
                                logger.info(f"  {row['region']}: {row['value_mean']:.2f} gha/person (n={row['value_count']:.0f})")
    
    return geo_aggs

def test_all_transformations():
    """
    Test all core transformations.
    """
    logger.info("Testing all core transformations...")
    transformer = FootprintCoreTransformer()
    
    # Run all transformations
    all_results = transformer.run_all_core_transformations()
    
    # Log summary
    logger.info("Core transformations completed")
    logger.info(f"Number of transformed datasets: {len(all_results)}")
    for name, df in all_results.items():
        if not isinstance(df, pd.DataFrame):
            continue
        if not df.empty:
            logger.info(f"  {name}: {len(df)} rows, {len(df.columns)} columns")
    
    return all_results

if __name__ == "__main__":
    logger.info("Starting core transformations test")
    
    try:
        # Test all transformations
        results = test_all_transformations()
        
        # If you want to test individual components:
        # dimensions = test_dimension_tables()
        # measures = test_fact_table()
        # indicators = test_ecological_indicators()
        # time_series = test_time_series()
        # geo_aggs = test_geographical_aggregations()
        
        logger.info("Core transformations test completed successfully")
    except Exception as e:
        logger.error(f"Error in core transformations test: {str(e)}")
        raise
