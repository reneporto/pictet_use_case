#!/usr/bin/env python3
"""
Quick test of the footprint network pipeline with limited data
"""

import os
import sys
from pathlib import Path

# Mock the airflow context for running outside of Airflow
class MockTaskInstance:
    def xcom_push(self, key, value):
        print(f"XCOM Push: {key} = {value}")
    
    def xcom_pull(self, key):
        print(f"XCOM Pull: {key}")
        return None

# Add current directory and dags to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))
sys.path.insert(0, str(current_dir / 'dags'))

# Import DAG functions
from footprint_network_pipeline_dag import extract_data, transform_data, load_data

def main():
    """
    Test the DAG functions with limited scope
    """
    
    # Mock Airflow context with limited parameters for quick testing
    context = {
        'ti': MockTaskInstance(),
        'dag_run': type('MockDagRun', (), {
            'conf': {
                'start_year': 2020,  # Only test 3 recent years
                'end_year': 2022,
                'countries': 'USA,CAN,MEX',  # Only test 3 countries
                'record_types': 'BiocapTotGHA,EcofootTotGHA',  # Only 2 record types
                'mode': 'full',
                'api_username': os.getenv('FOOTPRINT_NETWORK_USERNAME'),
                'api_key': os.getenv('FOOTPRINT_NETWORK_API_KEY')
            }
        })(),
        'params': {
            'start_year': 2020,
            'end_year': 2022,
            'countries': 'USA,CAN,MEX',
            'record_types': 'BiocapTotGHA,EcofootTotGHA',
            'mode': 'full',
            'api_username': os.getenv('FOOTPRINT_NETWORK_USERNAME'),
            'api_key': os.getenv('FOOTPRINT_NETWORK_API_KEY')
        }
    }
    
    print("🧪 Testing Footprint Network Pipeline with LIMITED data (3 countries, 3 years)")
    print("=" * 70)
    
    try:
        print("Step 1: Extract Data (Limited scope)")
        extraction_result = extract_data(**context)
        print(f"✅ Extraction completed successfully!")
        print(f"📊 Result summary: {type(extraction_result)}")
        
        print("\nStep 2: Transform Data")
        transformation_result = transform_data(**context)
        print(f"✅ Transformation completed successfully!")
        
        print("\nStep 3: Load Data")
        load_result = load_data(**context)
        print(f"✅ Load completed successfully!")
        
        print("\n🎉 PIPELINE TEST SUCCESSFUL!")
        print("=" * 70)
        print("The pipeline works! You can now run with full parameters if needed.")
        
    except Exception as e:
        print(f"❌ Error during pipeline execution: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
