#!/usr/bin/env python
"""
DuckDB Importer Utility
=======================
Standalone utility for efficiently importing Parquet files into DuckDB.
This utility is decoupled from the data transformation process and can be run
independently to load transformed data into DuckDB.
"""

import os
import glob
import logging
import argparse
from datetime import datetime

# Make the DuckDB import more resilient
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    logging.warning("DuckDB package is not available. Import operations will fail.")
    # Create a placeholder for type hints to avoid errors
    class DuckDBPlaceholder:
        def __init__(self, *args, **kwargs):
            raise ImportError("DuckDB is not installed. Please install it with 'pip install duckdb'")
        
        def __getattr__(self, name):
            raise ImportError("DuckDB is not installed. Please install it with 'pip install duckdb'")
    
    duckdb = DuckDBPlaceholder()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DuckDBParquetImporter:
    """Class for importing Parquet files into DuckDB."""
    
    def __init__(self, db_path=None, create_if_not_exists=True, multithreading=True):
        """
        Initialize the DuckDB importer.
        
        Args:
            db_path: Path to the DuckDB database file
            create_if_not_exists: Whether to create the database if it doesn't exist
            multithreading: Whether to use multithreading for import operations
        """
        # Check if DuckDB is available
        if not DUCKDB_AVAILABLE:
            logger.warning("DuckDB is not installed. This is fine for DAG loading but will fail during execution.")
            self.conn = None
            return
            
        self.db_path = db_path
        self.multithreading = multithreading
        
        # Connect to DuckDB
        self.conn = duckdb.connect(db_path, read_only=False) if db_path else duckdb.connect()
        
        # Configure multithreading
        if self.multithreading:
            self.conn.execute("PRAGMA threads=8")  # Use 8 threads for parallel processing
        
        logger.info(f"Initialized DuckDB importer{'with multithreading' if multithreading else ''}")

    def connect(self):
        """Establish connection to DuckDB database."""
        # Check if DuckDB is available
        if not DUCKDB_AVAILABLE:
            logger.error("Cannot connect: DuckDB is not installed.")
            raise ImportError("DuckDB is not installed. Please install it with 'pip install duckdb'")
            
        logger.info(f"Connecting to DuckDB at {self.db_path}")
        try:
            self.conn = duckdb.connect(self.db_path)
            logger.info("Connected to DuckDB")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {str(e)}")
            return False
        
    def close(self):
        """Close the DuckDB connection."""
        if DUCKDB_AVAILABLE and self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Closed DuckDB connection")
    
    def import_parquet(self, parquet_path, table_name, if_exists="replace"):
        """
        Import a Parquet file into DuckDB.
        
        Args:
            parquet_path: Path to the Parquet file
            table_name: Name of the target table
            if_exists: What to do if the table already exists ('replace' or 'append')
            
        Returns:
            Number of rows imported
        """
        # Check if DuckDB is available
        if not DUCKDB_AVAILABLE:
            logger.error("Cannot import Parquet: DuckDB is not installed.")
            raise ImportError("DuckDB is not installed. Please install it with 'pip install duckdb'")
            
        if if_exists == "replace":
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")
        else:  # append
            # Check if table exists
            result = self.conn.execute(f"SELECT name FROM information_schema.tables WHERE table_name = '{table_name}'")
            if result.fetchone() is None:
                # Table doesn't exist, create it
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")
            else:
                # Table exists, append to it
                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM read_parquet('{parquet_path}')")
        
        # Get row count
        result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = result.fetchone()[0]
        
        logger.info(f"Imported {row_count} rows into table '{table_name}' from {parquet_path}")
        return row_count
    
    def batch_import_directory(self, directory, file_pattern="*.parquet", table_mapping=None, 
                              timestamp=None, transaction=True):
        """
        Batch import multiple Parquet files from a directory.
        
        Parameters:
        -----------
        directory : str
            Directory containing Parquet files
        file_pattern : str
            Glob pattern to match files
        table_mapping : dict
            Optional mapping of file prefixes to table names
        timestamp : str
            Optional specific timestamp to filter files
        transaction : bool
            Whether to wrap imports in a transaction for atomicity
        """
        if not self.conn:
            self.connect()
            
        results = {}
        
        # Get all matching files
        if timestamp:
            pattern = os.path.join(directory, f"*_{timestamp}.parquet")
        else:
            pattern = os.path.join(directory, file_pattern)
            
        files = glob.glob(pattern)
        logger.info(f"Found {len(files)} files matching pattern in {directory}")
        
        if transaction:
            self.conn.execute("BEGIN TRANSACTION")
            
        try:
            for file_path in files:
                file_name = os.path.basename(file_path)
                # Extract the prefix (everything before the timestamp)
                prefix = file_name.split('_20')[0]  # Assumes timestamps start with '20'
                
                # Determine table name
                if table_mapping and prefix in table_mapping:
                    table_name = table_mapping[prefix]
                else:
                    # Default: use prefix as table name
                    table_name = prefix
                    
                try:
                    row_count = self.import_parquet(file_path, table_name)
                    results[table_name] = {
                        'file': file_name,
                        'rows': row_count,
                        'status': 'success'
                    }
                    logger.info(f"Imported {row_count} rows into {table_name} from {file_name}")
                except Exception as e:
                    results[table_name] = {
                        'file': file_name,
                        'rows': 0,
                        'status': f'error: {str(e)}'
                    }
                    logger.error(f"Failed to import {file_name} into {table_name}: {str(e)}")
                    if transaction:
                        # Rollback on any error to maintain atomicity
                        self.conn.execute("ROLLBACK")
                        return results
            
            if transaction:
                self.conn.execute("COMMIT")
                logger.info("Transaction committed successfully")
                
            return results
            
        except Exception as e:
            if transaction:
                self.conn.execute("ROLLBACK")
            logger.error(f"Error during batch import: {str(e)}")
            raise
    
    def create_views(self, views_dict):
        """
        Create analytical views on top of imported tables.
        
        Parameters:
        -----------
        views_dict : dict
            Dictionary mapping view names to their SQL definitions
        """
        if not self.conn:
            self.connect()
            
        for view_name, sql in views_dict.items():
            try:
                logger.info(f"Creating view: {view_name}")
                self.conn.execute(f"DROP VIEW IF EXISTS {view_name}")
                self.conn.execute(f"CREATE VIEW {view_name} AS {sql}")
            except Exception as e:
                logger.error(f"Error creating view {view_name}: {str(e)}")
    
    def create_indexes(self, indexes_dict):
        """
        Create indexes for performance optimization.
        
        Parameters:
        -----------
        indexes_dict : dict
            Dictionary mapping index names to tuples of (table, column)
        """
        if not self.conn:
            self.connect()
            
        for index_name, (table, column) in indexes_dict.items():
            try:
                logger.info(f"Creating index: {index_name} on {table}({column})")
                self.conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table}({column})")
            except Exception as e:
                logger.error(f"Error creating index {index_name}: {str(e)}")


def main():
    """Command line entry point for importing Parquet files into DuckDB."""
    parser = argparse.ArgumentParser(description="Import Parquet files into DuckDB")
    parser.add_argument("--db", required=True, help="DuckDB database path")
    parser.add_argument("--data-dir", required=True, help="Directory containing Parquet files")
    parser.add_argument("--timestamp", help="Specific timestamp to import")
    parser.add_argument("--no-transaction", action="store_true", help="Disable transaction wrapping")
    args = parser.parse_args()
    
    # Standard mapping for Global Footprint Network data
    table_mapping = {
        'dim_countries': 'countries',
        'dim_years': 'years',
        'dim_record_types': 'record_types',
        'fact_ecological_measures': 'ecological_measures',
        'indicator_ecological_balance': 'ecological_balance',
        'indicator_footprint_composition': 'footprint_composition',
        'indicator_time_series_changes': 'time_series_changes',
        'agg_by_region': 'region_aggregations',
        'agg_by_income': 'income_aggregations',
        'agg_population_weighted': 'weighted_aggregations'
    }
    
    # Initialize importer
    importer = DuckDBParquetImporter(args.db)
    
    try:
        # Import all files
        start_time = datetime.now()
        results = importer.batch_import_directory(
            args.data_dir, 
            table_mapping=table_mapping,
            timestamp=args.timestamp,
            transaction=not args.no_transaction
        )
        
        # Create useful indexes
        indexes = {
            "idx_ecological_balance_country": ("ecological_balance", "country_code"),
            "idx_ecological_measures_record": ("ecological_measures", "record")
        }
        importer.create_indexes(indexes)
        
        # Create analytical views
        views = {
            "v_country_footprint_summary": """
                SELECT c.country_name, c.region, c.income_group,
                       eb.year, eb.biocapacity, eb.footprint, eb.ecological_balance,
                       fc.carbon_pct, fc.crop_land_pct
                FROM ecological_balance eb
                JOIN countries c ON eb.country_code = c.country_code
                LEFT JOIN footprint_composition fc 
                    ON eb.country_code = fc.country_code AND eb.year = fc.year
            """
        }
        importer.create_views(views)
        
        # Report time taken
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Import completed in {duration:.2f} seconds")
        
        # Report summary
        success_count = sum(1 for r in results.values() if r['status'] == 'success')
        error_count = sum(1 for r in results.values() if 'error' in r['status'])
        logger.info(f"Import summary: {success_count} tables imported successfully, {error_count} errors")
        
    finally:
        importer.close()


if __name__ == "__main__":
    main()
