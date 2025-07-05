"""
Test script for verifying data transformation functionality.
"""
import os
import sys
import logging
from datetime import datetime

# Add the parent directory to the path to import the utils
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.data_transformer import FootprintDataTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger('test_data_transformer')

def test_transform_all_data():
    """
    Test transforming all data from JSON files into pandas DataFrames.
    """
    # Create the data transformer
    data_transformer = FootprintDataTransformer()
    
    # Transform all data
    logger.info("Transforming all data into pandas DataFrames...")
    dfs = data_transformer.transform_all_data()
    
    # Log the results
    for key, df in dfs.items():
        if not df.empty:
            logger.info(f"Successfully created DataFrame for {key} with {len(df)} rows and {len(df.columns)} columns")
            logger.info(f"Columns: {', '.join(df.columns)}")
            logger.info(f"Sample data:\n{df.head(3)}")
        else:
            logger.warning(f"DataFrame for {key} is empty")
    
    return dfs

def test_create_analytics_view():
    """
    Test creating an analytics view by joining the DataFrames.
    """
    # Create the data transformer
    data_transformer = FootprintDataTransformer()
    
    # Load data if available, otherwise transform it
    dfs = test_transform_all_data()
    
    # Create analytics view
    logger.info("\nCreating analytics view...")
    analytics_df = data_transformer.create_analytics_view(dfs)
    
    if not analytics_df.empty:
        logger.info(f"Successfully created analytics view with {len(analytics_df)} rows and {len(analytics_df.columns)} columns")
        logger.info(f"Columns: {', '.join(analytics_df.columns)}")
        logger.info(f"Sample data:\n{analytics_df.head(3)}")
        
        # Run some sample analytics
        logger.info("\nRunning sample analytics:")
        
        # Countries with highest biocapacity per person
        try:
            biocap_per_cap = analytics_df[analytics_df['record'] == 'BiocapPerCap'].sort_values('value', ascending=False)
            logger.info("\nTop 5 countries by biocapacity per person:")
            for _, row in biocap_per_cap.head(5).iterrows():
                logger.info(f"  {row['country_name']}: {row['value']:.2f} global hectares per capita")
        except Exception as e:
            logger.warning(f"Biocapacity per person analytics failed: {str(e)}")
        
        # Countries with highest ecological footprint per person
        try:
            ef_per_cap = analytics_df[analytics_df['record'] == 'EFConsPerCap'].sort_values('value', ascending=False)
            logger.info("\nTop 5 countries by ecological footprint per person:")
            for _, row in ef_per_cap.head(5).iterrows():
                logger.info(f"  {row['country_name']}: {row['value']:.2f} global hectares per capita")
        except Exception as e:
            logger.warning(f"Ecological footprint per person analytics failed: {str(e)}")
        
        # Countries with highest carbon footprint
        try:
            carbon_footprint = analytics_df[analytics_df['record'] == 'EFConsPerCap'].sort_values('carbon', ascending=False)
            logger.info("\nTop 5 countries by carbon footprint:")
            for _, row in carbon_footprint.head(5).iterrows():
                logger.info(f"  {row['country_name']}: {row['carbon']:.2f} global hectares per capita")
        except Exception as e:
            logger.warning(f"Carbon footprint analytics failed: {str(e)}")
    else:
        logger.warning("Analytics view is empty")

if __name__ == "__main__":
    logger.info("Starting data transformer test")
    
    try:
        # Test creating analytics view
        test_create_analytics_view()
        
        logger.info("Data transformer test completed successfully")
    except Exception as e:
        logger.error(f"Error in data transformer test: {str(e)}")
        raise
