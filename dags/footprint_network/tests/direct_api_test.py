"""
Direct API test script to troubleshoot connection issues.
"""

import requests
import json
import sys
from pathlib import Path

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
dotenv_path = Path(__file__).parent.parent / ".env"
if dotenv_path.exists():
    load_dotenv(dotenv_path=str(dotenv_path))

# Get the API key from environment variables
API_KEY = os.environ.get("FOOTPRINT_API_KEY", "")

# Different URL variations to try
urls_to_try = [
    "http://data.footprintnetwork.org/api/countries",
    "https://data.footprintnetwork.org/api/countries",
    "http://data.footprintnetwork.org/countries",
    "https://data.footprintnetwork.org/countries",
    "http://api.footprintnetwork.org/v1/countries"
]

# Different ways to authenticate
auth_methods = [
    # Query parameter
    lambda url, key: f"{url}?api_key={key}",
    # Header as Bearer token
    lambda url, key: (url, {"Authorization": f"Bearer {key}"}),
    # Header as API key
    lambda url, key: (url, {"X-Api-Key": key}),
    # Basic auth
    lambda url, key: (url, None, (key, "")),
]

print("Testing Global Footprint Network API with different configurations...")

for url in urls_to_try:
    print(f"\nTrying URL: {url}")
    
    # Try without authentication first
    try:
        print("  Without authentication:")
        response = requests.get(url, timeout=10)
        print(f"    Status: {response.status_code}")
        if response.status_code == 200:
            print("    Success! This URL works without authentication")
            try:
                print(f"    Response sample: {response.text[:100]}...")
            except:
                print("    Could not display response")
        else:
            print(f"    Failed with status {response.status_code}: {response.reason}")
    except Exception as e:
        print(f"    Error: {str(e)}")

    # Try with different authentication methods
    for i, auth_method in enumerate(auth_methods):
        try:
            print(f"  Authentication method {i+1}:")
            result = auth_method(url, API_KEY)
            
            if isinstance(result, tuple) and len(result) == 3:
                # Basic auth
                auth_url, headers, auth = result
                response = requests.get(auth_url, headers=headers, auth=auth, timeout=10)
            elif isinstance(result, tuple) and len(result) == 2:
                # Header auth
                auth_url, headers = result
                response = requests.get(auth_url, headers=headers, timeout=10)
            else:
                # Query param auth
                auth_url = result
                response = requests.get(auth_url, timeout=10)
                
            print(f"    Status: {response.status_code}")
            if response.status_code == 200:
                print("    Success! This authentication method works")
                try:
                    print(f"    Response sample: {response.text[:100]}...")
                except:
                    print("    Could not display response")
            else:
                print(f"    Failed with status {response.status_code}: {response.reason}")
                
        except Exception as e:
            print(f"    Error: {str(e)}")

print("\nAPI testing complete.")
