"""
Test script for verifying DuckDB functionality.
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger('test_duckdb')

def load_test_data():
    """
    Load some test data into the database.
    """
    db = FootprintDuckDBManager()
    
    # Create tables
    db.create_tables()
    
    # Insert some test years
    years = [2020, 2021, 2022, 2023, 2024]
    for year in years:
        db.execute_query("INSERT INTO years VALUES (?) ON CONFLICT DO NOTHING", (year,))
    
    # Insert some test countries
    countries = [
        (1, "United States", "USA", "US"),
        (2, "Afghanistan", "Afghanistan", "AF"),
        (3, "Albania", "Albania", "AL")
    ]
    for country in countries:
        db.execute_query(
            "INSERT INTO countries VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING", 
            country
        )
    
    # Insert some test record types
    record_types = [
        ("BCpc", "Biocapacity per person", "Biocapacity divided by population in global hectares (gha)", "BiocapPerCap"),
        ("EFCpc", "Ecological Footprint per person", "Ecological Footprint of consumption in global hectares (gha) divided by population", "EFConsPerCap"),
        ("pop", "Population", "Population count", "Population")
    ]
    for record_type in record_types:
        db.execute_query(
            "INSERT INTO record_types VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING", 
            record_type
        )
    
    logger.info("Test data loaded successfully")
    
    return db

def run_test_queries(db):
    """
    Run some test queries to verify the database is working.
    """
    # Test query 1: Count the years
    result = db.execute_query("SELECT COUNT(*) FROM years").fetchall()
    logger.info(f"Number of years: {result[0][0]}")
    
    # Test query 2: List all countries
    result = db.execute_query("SELECT country_code, country_name, iso_a2 FROM countries").fetchall()
    logger.info("Countries in the database:")
    for row in result:
        logger.info(f"  {row[0]}: {row[1]} ({row[2]})")
    
    # Test query 3: Join query (even though there's no data yet)
    query = """
    SELECT c.country_name, y.year, rt.name, rt.code
    FROM countries c
    CROSS JOIN years y
    CROSS JOIN record_types rt
    LIMIT 10
    """
    result = db.execute_query(query).fetchall()
    logger.info("Sample cross join (countries, years, record_types):")
    for row in result:
        logger.info(f"  {row[0]}, {row[1]}, {row[2]} ({row[3]})")

if __name__ == "__main__":
    logger.info("Starting DuckDB test")
    
    try:
        # Load test data
        db = load_test_data()
        
        # Run test queries
        run_test_queries(db)
        
        # Close the connection
        db.close()
        logger.info("DuckDB test completed successfully")
    except Exception as e:
        logger.error(f"Error in DuckDB test: {str(e)}")
        raise
