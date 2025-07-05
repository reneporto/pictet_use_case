#!/usr/bin/env python
"""
Global Footprint Network - DuckDB Import Script
===============================================
This script imports all transformed data files into DuckDB for analytics.
"""

import os
import sys
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.duckdb_importer import DuckDBParquetImporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 
            'logs', 
            f"duckdb_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        ))
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main execution function to import transformed data into DuckDB."""
    
    # Define paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, 'data', 'transformed')
    db_path = os.path.join(base_dir, 'data', 'footprint_network.duckdb')
    
    # Make sure log directory exists
    os.makedirs(os.path.join(base_dir, 'logs'), exist_ok=True)
    
    # Table mapping (transformed file prefix -> database table name)
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
    
    # Initialize the importer
    importer = DuckDBParquetImporter(db_path)
    
    try:
        logger.info(f"Starting import of transformed data into DuckDB at {db_path}")
        
        # Import all transformed data (uses the latest files unless timestamp specified)
        results = importer.batch_import_directory(
            data_dir, 
            table_mapping=table_mapping,
            transaction=True  # Use transaction for atomic operation
        )
        
        # Create useful indexes for query performance
        indexes = {
            "idx_ecological_balance_country": ("ecological_balance", "country_code"),
            "idx_ecological_measures_record": ("ecological_measures", "record"),
            "idx_ecological_measures_country_year": ("ecological_measures", "country_code, year")
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
            """,
            
            "v_regional_trends": """
                SELECT ra.region, ra.year, 
                       ra.record,
                       ra.value_mean, ra.value_median, ra.value_std
                FROM region_aggregations ra
                WHERE ra.record IN ('BiocapPerCap', 'EFConsPerCap')
                ORDER BY ra.region, ra.record, ra.year
            """,
            
            "v_income_group_comparison": """
                SELECT ia.income_group, ia.year, 
                       ia.record,
                       ia.value_mean, ia.value_median, ia.value_std
                FROM income_aggregations ia
                WHERE ia.record IN ('BiocapPerCap', 'EFConsPerCap')
                ORDER BY ia.income_group, ia.record, ia.year
            """
        }
        importer.create_views(views)
        
        # Print summary of import results
        success_count = sum(1 for r in results.values() if r['status'] == 'success')
        total_rows = sum(r['rows'] for r in results.values() if r['status'] == 'success')
        
        logger.info(f"Import completed: {success_count}/{len(results)} tables imported successfully")
        logger.info(f"Total rows imported: {total_rows}")
        
        # Print details for each table
        for table_name, result in results.items():
            status = "✅ " if result['status'] == 'success' else "❌ "
            logger.info(f"{status}{table_name}: {result['rows']} rows from {result['file']}")
            
    except Exception as e:
        logger.error(f"Error during import process: {str(e)}", exc_info=True)
    finally:
        importer.close()
        logger.info("Import process finished")


if __name__ == "__main__":
    main()
