"""
Test script for verifying data loader functionality.
"""
import os
import sys
import json
import logging
from datetime import datetime

# Add the parent directory to the path to import the utils
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from utils.db_manager import FootprintDuckDBManager
from utils.data_loader import FootprintDataLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger('test_data_loader')

def test_load_all_data():
    """
    Test loading all data from JSON files into DuckDB.
    """
    # Create a new database manager and ensure tables are created
    db_manager = FootprintDuckDBManager()
    db_manager.create_tables()
    
    # Create the data loader
    data_loader = FootprintDataLoader(db_manager)
    
    # Load all data
    logger.info("Loading all data into DuckDB...")
    results = data_loader.load_all_data()
    
    # Log the results
    logger.info(f"Loaded {results['countries']} countries")
    logger.info(f"Loaded {results['years']} years")
    logger.info(f"Loaded {results['record_types']} record types")
    logger.info(f"Loaded {results['ecological_measures']} ecological measures")
    
    # Run some validation queries
    logger.info("\nRunning validation queries:")
    
    # Check countries
    countries_count = db_manager.execute_query("SELECT COUNT(*) FROM countries").fetchone()[0]
    logger.info(f"Countries in database: {countries_count}")
    
    # Check years
    years_count = db_manager.execute_query("SELECT COUNT(*) FROM years").fetchone()[0]
    logger.info(f"Years in database: {years_count}")
    
    # Check record types
    record_types_count = db_manager.execute_query("SELECT COUNT(*) FROM record_types").fetchone()[0]
    logger.info(f"Record types in database: {record_types_count}")
    
    # Check ecological measures
    measures_count = db_manager.execute_query("SELECT COUNT(*) FROM ecological_measures").fetchone()[0]
    logger.info(f"Ecological measures in database: {measures_count}")
    
    # Sample query: Get top countries by biocapacity per person in the most recent year
    logger.info("\nRunning sample analytical query:")
    query = """
    SELECT c.country_name, c.iso_a2, em.year, em.value as biocap_per_capita
    FROM ecological_measures em
    JOIN countries c ON em.country_code = c.country_code
    WHERE em.record = 'BiocapPerCap'
    AND em.year = (SELECT MAX(year) FROM ecological_measures WHERE record = 'BiocapPerCap')
    ORDER BY em.value DESC
    LIMIT 10
    """
    
    try:
        results = db_manager.execute_query(query).fetchall()
        logger.info("Top 10 countries by biocapacity per person:")
        for row in results:
            logger.info(f"  {row[0]} ({row[1]}): {row[2]}, {row[3]:.2f} global hectares per capita")
    except Exception as e:
        logger.warning(f"Sample query failed: {str(e)}")
    
    return db_manager

def test_filtered_loading():
    """
    Test loading data for specific countries or years.
    """
    # Create a new database manager
    db_manager = FootprintDuckDBManager()
    
    # Create the data loader
    data_loader = FootprintDataLoader(db_manager)
    
    # Load data for Afghanistan (country code 2) in 2023
    logger.info("\nLoading data for Afghanistan (2) in 2023...")
    count = data_loader.load_ecological_measures(country_code="2", year=2023)
    logger.info(f"Loaded {count} measures for Afghanistan in 2023")
    
    # Run a query to verify
    query = """
    SELECT c.country_name, em.year, rt.name, em.value
    FROM ecological_measures em
    JOIN countries c ON em.country_code = c.country_code
    JOIN record_types rt ON em.record = rt.record
    WHERE c.country_code = 2 AND em.year = 2023
    LIMIT 5
    """
    
    try:
        results = db_manager.execute_query(query).fetchall()
        logger.info("Sample of Afghanistan data in 2023:")
        for row in results:
            logger.info(f"  {row[0]}, {row[1]}, {row[2]}: {row[3]}")
    except Exception as e:
        logger.warning(f"Query failed: {str(e)}")
    
    return db_manager

if __name__ == "__main__":
    logger.info("Starting data loader test")
    
    try:
        # Test loading all data
        db_manager = test_load_all_data()
        
        # Test filtered loading
        test_filtered_loading()
        
        # Close the connection
        db_manager.close()
        logger.info("Data loader test completed successfully")
    except Exception as e:
        logger.error(f"Error in data loader test: {str(e)}")
        raise
