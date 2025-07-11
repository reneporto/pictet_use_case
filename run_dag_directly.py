#!/usr/bin/env python3
"""
Direct execution script for footprint_network_pipeline_dag functions
This script allows running the DAG tasks without Airflow scheduler
"""

import sys
import os
from datetime import datetime
import logging

# Add the dags directory to the path
sys.path.append('/usr/local/airflow/dags')
os.chdir('/usr/local/airflow/dags')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_mock_context():
    """Create a mock Airflow context for task execution"""
    mock_ti = type('MockTaskInstance', (), {
        'xcom_push': lambda self, key, value: print(f"XCOM Push: {key} = {value}"),
        'xcom_pull': lambda self, key, task_ids=None: print(f"XCOM Pull: {key} from {task_ids}") or None
    })()
    
    return {
        'ds': datetime.now().strftime('%Y-%m-%d'),
        'ds_nodash': datetime.now().strftime('%Y%m%d'),
        'execution_date': datetime.now(),
        'ti': mock_ti,
        'task_instance': mock_ti,
        'dag_run': type('MockDagRun', (), {
            'conf': {
                'start_year': 1961,
                'end_year': 2023,
                'countries': 'all',
                'record_types': 'all',
                'mode': 'full',
                'api_username': os.getenv('FOOTPRINT_NETWORK_USERNAME'),
                'api_key': os.getenv('FOOTPRINT_NETWORK_API_KEY')
            }
        })(),
        'params': {
            'start_year': 1961,
            'end_year': 2023,
            'countries': 'all',
            'record_types': 'all',
            'mode': 'full',
            'api_username': os.getenv('FOOTPRINT_NETWORK_USERNAME'),
            'api_key': os.getenv('FOOTPRINT_NETWORK_API_KEY')
        }
    }

def main():
    logger.info("Starting direct execution of footprint_network_pipeline_dag functions")
    
    try:
        # Import the DAG functions
        from footprint_network_pipeline_dag import extract_data, transform_data, load_data, get_base_paths
        
        # Create mock context
        context = create_mock_context()
        
        # Step 1: Get base paths
        logger.info("Step 1: Setting up base paths")
        get_base_paths()
        
        # Step 2: Extract data
        logger.info("Step 2: Extracting data from Global Footprint Network API")
        extract_data(**context)
        
        # Step 3: Transform data
        logger.info("Step 3: Transforming raw data into analytical formats")
        transform_data(**context)
        
        # Step 4: Load data (if DuckDB is available)
        logger.info("Step 4: Loading transformed data into DuckDB")
        load_data(**context)
        
        logger.info("Pipeline execution completed successfully!")
        
    except ImportError as e:
        logger.error(f"Import error: {e}")
        logger.info("This might be due to missing dependencies. Let's try running individual components.")
        
    except Exception as e:
        logger.error(f"Error during pipeline execution: {e}")
        logger.info("Check the logs above for more details about the error.")

if __name__ == "__main__":
    main()
