"""
DuckDB integration for the footprint network data.
"""
import os
import logging
import duckdb
from datetime import datetime
from typing import Optional

# Set up logging
logger = logging.getLogger(__name__)

class FootprintDuckDBManager:
    """
    Class for managing DuckDB database operations for footprint data.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the DuckDB manager.
        
        Args:
            db_path (str, optional): Path to the DuckDB database file.
                                     If None, a default path is used.
        """
        if db_path is None:
            # Create a database in the data directory
            data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'db')
            os.makedirs(data_dir, exist_ok=True)
            db_path = os.path.join(data_dir, 'footprint_network.duckdb')
        
        self.db_path = db_path
        logger.info(f"Initializing DuckDB at {db_path}")
        self.conn = duckdb.connect(db_path)
        
    def execute_query(self, query: str, params=None):
        """
        Execute a SQL query.
        
        Args:
            query (str): SQL query to execute
            params (tuple, optional): Parameters for the query
            
        Returns:
            The query result
        """
        logger.debug(f"Executing query: {query}")
        try:
            if params:
                result = self.conn.execute(query, params)
            else:
                result = self.conn.execute(query)
            return result
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def create_tables(self):
        """
        Create the necessary tables in the database if they don't exist.
        """
        # Create countries table
        self.execute_query("""
        CREATE TABLE IF NOT EXISTS countries (
            country_code INTEGER PRIMARY KEY,
            country_name VARCHAR,
            short_name VARCHAR,
            iso_a2 VARCHAR(2)
        )
        """)
        
        # Create years table
        self.execute_query("""
        CREATE TABLE IF NOT EXISTS years (
            year INTEGER PRIMARY KEY
        )
        """)
        
        # Create record types table
        self.execute_query("""
        CREATE TABLE IF NOT EXISTS record_types (
            code VARCHAR PRIMARY KEY,
            name VARCHAR,
            note TEXT,
            record VARCHAR
        )
        """)
        
        # Create ecological measures table
        self.execute_query("""
        CREATE TABLE IF NOT EXISTS ecological_measures (
            country_code INTEGER,
            year INTEGER,
            record VARCHAR,
            crop_land DOUBLE,
            grazing_land DOUBLE,
            forest_land DOUBLE,
            fishing_ground DOUBLE,
            builtup_land DOUBLE,
            carbon DOUBLE,
            value DOUBLE,
            score VARCHAR,
            loaded_at TIMESTAMP,
            PRIMARY KEY (country_code, year, record),
            FOREIGN KEY (country_code) REFERENCES countries(country_code),
            FOREIGN KEY (year) REFERENCES years(year),
            FOREIGN KEY (record) REFERENCES record_types(code)
        )
        """)
        
        logger.info("Database tables created successfully")
    
    def close(self):
        """
        Close the database connection.
        """
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
