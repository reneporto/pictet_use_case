"""
Global Footprint Network Data Pipeline DAG
=========================================

This DAG orchestrates the complete data pipeline for Global Footprint Network data:
1. Extract: Fetch data from the Global Footprint Network API
2. Transform: Process and transform raw data into analytical formats
3. Load: Import transformed data into DuckDB for analysis

The pipeline is parameterized to allow for different configurations:
- Start/end year ranges
- Country selection
- Record type filtering
- Full vs. incremental processing

Author: Data Engineering Team
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import os
import sys
import logging
import json
import time
import pandas as pd

# Load environment variables from .env file if it exists
env_file_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(env_file_path):
    with open(env_file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key] = value

# Add the footprint_network directory to the path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'footprint_network'))

# Import our custom modules
from footprint_network.utils.api_client import FootprintNetworkAPI
from footprint_network.utils.data_transformer_core import FootprintCoreTransformer as DataTransformer

# Make imports resilient to missing packages
try:
    import duckdb
    from footprint_network.utils.duckdb_importer import DuckDBParquetImporter
    from footprint_network.utils.db_manager import FootprintDuckDBManager as DuckDBManager
    DUCKDB_AVAILABLE = True
except ImportError:
    logging.warning("DuckDB is not installed. Load phase will be skipped.")
    DuckDBParquetImporter = None
    DuckDBManager = None
    DUCKDB_AVAILABLE = False

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email': ['reneporto.ie@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    #'retries': 2,
    #'retry_delay': timedelta(minutes=5),
    #'execution_timeout': timedelta(hours=2),
}

# DAG configuration
DAG_ID = 'footprint_network_pipeline'
SCHEDULE_INTERVAL = '@daily'  # Or use '0 0 * * *' for daily at midnight
START_DATE = datetime(2025, 1, 1)
TAGS = ['footprint_network', 'ecological_data', 'etl_pipeline']
CATCHUP = False

# Define the DAG
dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='End-to-end data pipeline for Global Footprint Network data',
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
    tags=TAGS,
    max_active_runs=1,  # Prevent multiple concurrent runs
    params={
        'start_year': 1961,  # Default start year
        'end_year': 2023,    # Default end year
        'countries': 'all',  # Default to all countries
        'record_types': 'all',  # Default to all record types
        'mode': 'full',      # 'full' or 'incremental'
        'api_username': os.getenv('FOOTPRINT_NETWORK_USERNAME', "{{ var.value.footprint_network_username }}"),
        'api_key': os.getenv('FOOTPRINT_NETWORK_API_KEY', "{{ var.value.footprint_network_api_key }}")
    }
)


# Helper function to get base directory paths
def get_base_paths():
    """Get base directory paths for data storage"""
    base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'footprint_network')
    data_dir = os.path.join(base_dir, 'data')
    raw_dir = os.path.join(data_dir, 'raw')
    processed_dir = os.path.join(data_dir, 'processed')
    transformed_dir = os.path.join(data_dir, 'transformed')
    db_path = os.path.join(data_dir, 'footprint_network.duckdb')
    
    # Ensure directories exist
    for directory in [data_dir, raw_dir, processed_dir, transformed_dir]:
        os.makedirs(directory, exist_ok=True)
    
    return {
        'base_dir': base_dir,
        'data_dir': data_dir,
        'raw_dir': raw_dir,
        'processed_dir': processed_dir,
        'transformed_dir': transformed_dir,
        'db_path': db_path
    }


# Task 1: Extract data from Global Footprint Network API
def extract_data(**kwargs):
    """Extract data from Global Footprint Network API and save as raw JSON"""
    # Get runtime parameters
    params = kwargs['params']
    ti = kwargs['ti']
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('extract_data')
    logger.info(f"Starting data extraction with params: {params}")
    
    # Get paths
    paths = get_base_paths()
    raw_dir = paths['raw_dir']
    
    # Extract parameters with environment variable fallbacks
    api_username = params.get('api_username')
    api_key = params.get('api_key')
    
    # If template variables weren't resolved, use environment variables
    if api_username and api_username.startswith('{{'):
        api_username = os.getenv('FOOTPRINT_NETWORK_USERNAME', 'your_username')
        logger.info("Using environment variable for API username")
    if api_key and api_key.startswith('{{'):
        api_key = os.getenv('FOOTPRINT_NETWORK_API_KEY', 'qmub4lan4698clu1pep591s845lkprn7p1lrj8j16bfksu5cd59')
        logger.info("Using environment variable for API key")
    
    start_year = params.get('start_year')
    end_year = params.get('end_year')
    countries_param = params.get('countries')
    
    # Initialize API client
    client = FootprintNetworkAPI(username=api_username, api_key=api_key)
    
    # Create timestamp for this extraction run
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Fetch available countries if needed
    if countries_param == 'all':
        logger.info("Fetching all available countries")
        countries_data = client.get_countries()
        countries = [country.get('countryCode', country.get('code', 'unknown')) for country in countries_data]
    else:
        if isinstance(countries_param, str):
            countries = [c.strip() for c in countries_param.split(',')]
        else:
            countries = countries_param
    
    logger.info(f"Will extract data for {len(countries)} countries")
    
    # Create results dictionary to track extraction
    extraction_results = {
        'timestamp': timestamp,
        'start_year': start_year,
        'end_year': end_year,
        'countries_count': len(countries),
        'countries': countries,
        'files': []
    }
    
    # Fetch data for each country and year
    total_records = 0
    for country_code in countries:
        country_file = os.path.join(raw_dir, f"country_{country_code}_{timestamp}.json")
        
        try:
            # Get country details
            country_details = client.get_country_data(country_code)
            
            # Fetch data for each year
            country_data = {
                'country_code': country_code,
                'country_details': country_details,
                'years': {}
            }
            
            for year in range(start_year, end_year + 1):
                try:
                    # Fetch all record types for this country and year
                    year_data = client.get_data_for_country_year(country_code, year)
                    if year_data:
                        country_data['years'][str(year)] = year_data
                        total_records += len(year_data)
                except Exception as e:
                    logger.warning(f"Failed to fetch data for {country_code} in {year}: {str(e)}")
                    continue
            
            # Save country data to JSON file
            with open(country_file, 'w') as f:
                json.dump(country_data, f)
            
            extraction_results['files'].append({
                'country': country_code,
                'file': os.path.basename(country_file),
                'years': len(country_data['years']),
                'status': 'success'
            })
            
            logger.info(f"Successfully extracted data for {country_code}: {len(country_data['years'])} years")
            
        except Exception as e:
            logger.error(f"Failed to extract data for {country_code}: {str(e)}")
            extraction_results['files'].append({
                'country': country_code,
                'file': None,
                'years': 0,
                'status': f'error: {str(e)}'
            })
    
    # Save extraction summary
    summary_file = os.path.join(raw_dir, f"extraction_summary_{timestamp}.json")
    extraction_results['total_records'] = total_records
    extraction_results['timestamp'] = timestamp
    
    with open(summary_file, 'w') as f:
        json.dump(extraction_results, f)
    
    logger.info(f"Extraction completed: {total_records} records from {len(countries)} countries")
    
    # Push timestamp to XCom for downstream tasks
    ti.xcom_push(key='extraction_timestamp', value=timestamp)
    
    return timestamp


# Task 2: Transform raw data into analytical formats
def transform_data(**kwargs):
    """Transform raw JSON data into analytical Parquet files"""
    # Get runtime parameters
    params = kwargs['params']
    ti = kwargs['ti']
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('transform_data')
    
    # Get timestamp from previous task
    extraction_timestamp = ti.xcom_pull(task_ids='extract_data', key='extraction_timestamp')
    logger.info(f"Starting transformation of data extracted at {extraction_timestamp}")
    
    # Get paths
    paths = get_base_paths()
    raw_dir = paths['raw_dir']
    processed_dir = paths['processed_dir']
    transformed_dir = paths['transformed_dir']
    
    # Create timestamp for this transformation run
    transform_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Initialize the transformer
    transformer = DataTransformer()
    
    # Load extraction summary to get files to process
    summary_file = os.path.join(raw_dir, f"extraction_summary_{extraction_timestamp}.json")
    
    try:
        with open(summary_file, 'r') as f:
            extraction_summary = json.load(f)
    except FileNotFoundError:
        logger.error(f"Extraction summary file not found: {summary_file}")
        raise
    
    # Process the raw data
    logger.info("Processing raw data files...")
    
    # For this example we're using the core transformer methods directly
    # Note: In a real deployment, you might want to adapt this code to
    # your specific data loading process
    
    # Run all transformations
    logger.info("Running all core transformations...")
    results = transformer.run_all_core_transformations()
    
    # Extract results from the transformation
    dim_countries = results['dim_countries']
    dim_years = results['dim_years']
    dim_record_types = results['dim_record_types']
    fact_ecological_measures = results['fact_ecological_measures']
    indicator_ecological_balance = results['indicator_ecological_balance']
    indicator_footprint_composition = results['indicator_footprint_composition']
    indicator_time_series_changes = results['indicator_time_series_changes']
    agg_by_region = results['region_aggregations']
    agg_by_income = results['income_aggregations']
    agg_population_weighted = results['weighted_aggregations']
    
    # Save all transformed data using the _save_dataframe method
    for name, df in results.items():
        if isinstance(df, pd.DataFrame):
            file_path = os.path.join(transformed_dir, f"{name}_{transform_timestamp}.parquet")
            df.to_parquet(file_path, index=False)
            logger.info(f"Saved {name} with {len(df)} rows to {file_path}")
        else:
            logger.warning(f"Result {name} is not a DataFrame, skipping save")

    
    # Create a transformation summary
    transform_summary = {
        'extraction_timestamp': extraction_timestamp,
        'transform_timestamp': transform_timestamp,
        'dim_countries': len(dim_countries),
        'dim_years': len(dim_years),
        'dim_record_types': len(dim_record_types),
        'fact_ecological_measures': len(fact_ecological_measures),
        'indicator_ecological_balance': len(indicator_ecological_balance),
        'indicator_footprint_composition': len(indicator_footprint_composition),
        'indicator_time_series_changes': len(indicator_time_series_changes),
        'agg_by_region': len(agg_by_region),
        'agg_by_income': len(agg_by_income),
        'agg_population_weighted': len(agg_population_weighted),
    }
    
    # Save transformation summary
    summary_file = os.path.join(transformed_dir, f"transform_summary_{transform_timestamp}.json")
    with open(summary_file, 'w') as f:
        json.dump(transform_summary, f)
    
    logger.info(f"Transformation completed with timestamp {transform_timestamp}")
    
    # Push timestamp to XCom for downstream tasks
    ti.xcom_push(key='transform_timestamp', value=transform_timestamp)
    
    return transform_timestamp


# Task 3: Load transformed data into DuckDB
def load_data(**kwargs):
    """Load transformed Parquet data into DuckDB"""
    # Get runtime parameters
    params = kwargs['params']
    ti = kwargs['ti']
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, 
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('load_data')
    
    # Get timestamp from previous task
    transform_timestamp = ti.xcom_pull(task_ids='transform_data', key='transform_timestamp')
    logger.info(f"Starting loading of data transformed at {transform_timestamp}")
    
    # Get paths
    paths = get_base_paths()
    transformed_dir = paths['transformed_dir']
    db_path = paths['db_path']
    
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
    
    # Check if DuckDB is available
    if not DUCKDB_AVAILABLE:
        logger.warning("DuckDB is not installed. Skipping data loading phase.")
        # Create a load summary indicating skipped status
        load_summary = {
            'transform_timestamp': transform_timestamp,
            'load_timestamp': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'db_path': None,
            'status': 'skipped',
            'reason': 'DuckDB not installed',
            'message': 'To enable data loading, install DuckDB with pip install duckdb'
        }
        
        # Save load summary
        summary_file = os.path.join(transformed_dir, f"load_summary_{transform_timestamp}.json")
        with open(summary_file, 'w') as f:
            json.dump(load_summary, f)
        
        logger.info(f"Data loading skipped. Summary saved to {summary_file}")
        return load_summary
        
    # Initialize the DuckDB importer
    importer = DuckDBParquetImporter(db_path)
    
    try:
        logger.info(f"Starting import of transformed data into DuckDB at {db_path}")
        
        # Import all transformed data files with the specific timestamp
        results = importer.batch_import_directory(
            transformed_dir, 
            table_mapping=table_mapping,
            timestamp=transform_timestamp,
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
        
        # Calculate load statistics
        success_count = sum(1 for r in results.values() if r['status'] == 'success')
        total_rows = sum(r['rows'] for r in results.values() if r['status'] == 'success')
        
        # Create load summary
        load_summary = {
            'transform_timestamp': transform_timestamp,
            'load_timestamp': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'db_path': db_path,
            'tables_loaded': success_count,
            'total_rows': total_rows,
            'table_details': results
        }
        
        # Save load summary
        summary_file = os.path.join(transformed_dir, f"load_summary_{transform_timestamp}.json")
        with open(summary_file, 'w') as f:
            json.dump(load_summary, f)
        
        logger.info(f"Loading completed: {success_count}/{len(results)} tables with {total_rows} rows")
        
    except Exception as e:
        logger.error(f"Error during data loading: {str(e)}", exc_info=True)
        raise
    finally:
        importer.close()
        logger.info("Import process finished")
    
    return load_summary


# Define task groups for better organization in the UI
with TaskGroup(group_id="prepare_environment", dag=dag) as prepare_environment:
    # Task: Create necessary directories
    create_directories = BashOperator(
        task_id='create_directories',
        bash_command='''
        mkdir -p {{ ti.xcom_push(key='base_dir', value=dag_run.conf.get('base_dir', '/Users/reneporto/pictet/user_case_1/pictet_airflow/aws-mwaa-local-runner/dags/footprint_network')) }}/data/{raw,processed,transformed,plots,reports}
        mkdir -p {{ ti.xcom_pull(key='base_dir') }}/logs
        echo "Created directory structure at $(date)"
        ''',
        dag=dag
    )
    
    # Task: Check API credentials
    check_api_credentials = PythonOperator(
        task_id='check_api_credentials',
        python_callable=lambda **kwargs: all(
            x is not None and x != '' for x in 
            [kwargs['params'].get('api_username'), kwargs['params'].get('api_key')]
        ),
        dag=dag
    )
    
    create_directories >> check_api_credentials

# Extract task
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

# Transform task
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

# Load task
load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

# Define additional tasks for analysis and reporting
with TaskGroup(group_id="analysis_and_reporting", dag=dag) as analysis_and_reporting:
    # Task: Generate summary reports
    generate_reports = BashOperator(
        task_id='generate_reports',
        bash_command='''
        cd {{ ti.xcom_pull(key='base_dir') }}
        python -m utils.analysis.generate_reports \
            --transform-timestamp {{ ti.xcom_pull(task_ids='transform_data', key='transform_timestamp') }} \
            --output-dir data/reports
        echo "Generated reports at $(date)"
        ''',
        dag=dag
    )
    
    # Task: Generate visualization plots
    generate_plots = BashOperator(
        task_id='generate_plots',
        bash_command='''
        cd {{ ti.xcom_pull(key='base_dir') }}
        python -m utils.analysis.generate_plots \
            --transform-timestamp {{ ti.xcom_pull(task_ids='transform_data', key='transform_timestamp') }} \
            --output-dir data/plots
        echo "Generated plots at $(date)"
        ''',
        dag=dag
    )
    
    generate_reports >> generate_plots

# Define the task dependencies
prepare_environment >> extract_task >> transform_task >> load_task >> analysis_and_reporting

# Define on success callback to log completion
def on_success_callback(context):
    """Log successful completion of the DAG"""
    log_message = (
        f"Footprint Network ETL pipeline completed successfully at "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}!"
    )
    print(log_message)


# Add DAG-level success callback
dag.on_success_callback = on_success_callback

if __name__ == "__main__":
    dag.cli()
