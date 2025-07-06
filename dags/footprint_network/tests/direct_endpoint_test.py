"""
Direct endpoint testing script to diagnose authentication issues.
"""

import requests
import json
import sys
from pathlib import Path
import base64

# Add parent directory to Python path for imports
parent_dir = Path(__file__).parent.parent
sys.path.append(str(parent_dir))

from config.settings import API_BASE_URL, API_USERNAME, API_KEY

# Define test parameters
BASE_URL = "https://api.footprintnetwork.org/v1"
ENDPOINTS = [
    "/types",
    "/countries", 
    "/data?country=US&type=EFCpc&year=2019"
]

def test_endpoint_with_auth_methods(url):
    """Test an endpoint with different authentication methods."""
    print(f"\nTesting endpoint: {url}")
    
    # Method 1: Basic auth with username and API key
    try:
        print("\n1. Using basic auth with username and API key:")
        auth = (API_USERNAME, API_KEY)
        response = requests.get(url, auth=auth, timeout=10)
        print(f"  Status: {response.status_code}")
        if response.status_code == 200:
            print("  Success!")
            print(f"  Response sample: {response.text[:100]}...")
        else:
            print(f"  Failed: {response.reason}")
            print(f"  Response: {response.text[:200]}")
    except Exception as e:
        print(f"  Error: {str(e)}")
    
    # Method 2: Basic auth with API key only
    try:
        print("\n2. Using basic auth with API key only:")
        auth = (API_KEY, "")
        response = requests.get(url, auth=auth, timeout=10)
        print(f"  Status: {response.status_code}")
        if response.status_code == 200:
            print("  Success!")
            print(f"  Response sample: {response.text[:100]}...")
        else:
            print(f"  Failed: {response.reason}")
            print(f"  Response: {response.text[:200]}")
    except Exception as e:
        print(f"  Error: {str(e)}")
    
    # Method 3: Bearer token in Authorization header
    try:
        print("\n3. Using Bearer token in Authorization header:")
        headers = {"Authorization": f"Bearer {API_KEY}"}
        response = requests.get(url, headers=headers, timeout=10)
        print(f"  Status: {response.status_code}")
        if response.status_code == 200:
            print("  Success!")
            print(f"  Response sample: {response.text[:100]}...")
        else:
            print(f"  Failed: {response.reason}")
            print(f"  Response: {response.text[:200]}")
    except Exception as e:
        print(f"  Error: {str(e)}")
    
    # Method 4: API key in query param
    try:
        print("\n4. Using API key in query param:")
        query_url = f"{url}{'&' if '?' in url else '?'}api_key={API_KEY}"
        response = requests.get(query_url, timeout=10)
        print(f"  Status: {response.status_code}")
        if response.status_code == 200:
            print("  Success!")
            print(f"  Response sample: {response.text[:100]}...")
        else:
            print(f"  Failed: {response.reason}")
            print(f"  Response: {response.text[:200]}")
    except Exception as e:
        print(f"  Error: {str(e)}")
    
    # Method 5: API key as username with empty password
    try:
        print("\n5. Using API key as username with empty password and Accept header:")
        auth = (API_KEY, "")
        headers = {"Accept": "application/json"}
        response = requests.get(url, auth=auth, headers=headers, timeout=10)
        print(f"  Status: {response.status_code}")
        if response.status_code == 200:
            print("  Success!")
            print(f"  Response sample: {response.text[:100]}...")
        else:
            print(f"  Failed: {response.reason}")
            print(f"  Response: {response.text[:200]}")
    except Exception as e:
        print(f"  Error: {str(e)}")

def main():
    """Main test function."""
    print("Testing Global Footprint Network API endpoints with different auth methods...")
    
    # Test each endpoint with different auth methods
    for endpoint in ENDPOINTS:
        test_endpoint_with_auth_methods(f"{BASE_URL}{endpoint}")
    
    print("\nAPI testing complete.")

if __name__ == "__main__":
    main()
