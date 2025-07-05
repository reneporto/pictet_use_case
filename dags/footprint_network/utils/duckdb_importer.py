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
import duckdb

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DuckDBParquetImporter:
    """Fast, decoupled Parquet to DuckDB importer."""
    
    def __init__(self, db_path, create_if_not_exists=True):
        """Initialize the importer with DuckDB database path."""
        self.db_path = db_path
        self.conn = None
        self.create_if_not_exists = create_if_not_exists
        
    def connect(self):
        """Establish connection to DuckDB database."""
        logger.info(f"Connecting to DuckDB at {self.db_path}")
        self.conn = duckdb.connect(self.db_path, read_only=False)
        # Enable maximum parallelism for best performance
        self.conn.execute(f"SET threads TO {os.cpu_count()}")
        return self.conn
        
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("DuckDB connection closed")
    
    def import_parquet(self, parquet_path, table_name, if_exists="replace"):
        """
        Import a Parquet file directly into a DuckDB table - extremely efficient.
        
        Parameters:
        -----------
        parquet_path : str
            Path to the Parquet file
        table_name : str
            Name of the target table
        if_exists : str
            'replace' to drop and recreate (fastest), 'append' to add data
        """
        if not self.conn:
            self.connect()
            
        try:
            if if_exists == "replace":
                logger.info(f"Replacing table {table_name} with data from {os.path.basename(parquet_path)}")
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")
            else:
                # Check if table exists
                table_exists = self.conn.execute(
                    f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'"
                ).fetchone()[0] > 0
                
                if not table_exists and self.create_if_not_exists:
                    logger.info(f"Creating table {table_name} with data from {os.path.basename(parquet_path)}")
                    self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")
                else:
                    logger.info(f"Appending data from {os.path.basename(parquet_path)} to table {table_name}")
                    self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM read_parquet('{parquet_path}')")
            
            # Return row count
            count_result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            return count_result.fetchone()[0]
            
        except Exception as e:
            logger.error(f"Error importing {parquet_path} to {table_name}: {str(e)}")
            raise
    
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
