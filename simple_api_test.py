#!/usr/bin/env python3
"""
Simple API test to understand the correct country code format
"""

import os
import sys
from pathlib import Path

# Add parent directory to Python path for imports
parent_dir = Path(__file__).parent / 'dags'
sys.path.append(str(parent_dir))

from footprint_network.utils.api_client import FootprintNetworkAPI

def main():
    """
    Test to see country codes and understand API format
    """
    
    # Get API credentials from environment
    api_key = os.getenv('FOOTPRINT_NETWORK_API_KEY', 'qmub4lan4698clu1pep591s845lkprn7p1lrj8j16bfksu5cd59')
    username = os.getenv('FOOTPRINT_NETWORK_USERNAME', 'your_username')
    
    print(f"🔑 Using API Key: {api_key[:10]}...")
    print(f"👤 Using Username: {username}")
    
    # Create API client
    try:
        api = FootprintNetworkAPI(username=username, api_key=api_key)
        print("✅ API client created successfully")
        
        # Test getting countries
        print("\n📋 Fetching countries...")
        countries = api.get_countries()
        print(f"✅ Retrieved {len(countries)} countries")
        
        # Show first few countries with their codes
        print("\n🌍 Sample countries:")
        for i, country in enumerate(countries[:10]):
            code = country.get('countryCode', 'N/A')
            name = country.get('shortName', country.get('name', 'N/A'))
            iso = country.get('isoa2', 'N/A')
            print(f"  {i+1:2d}. {name:<20} | Code: {code:<4} | ISO: {iso}")
        
        # Try to find USA equivalent
        print("\n🔍 Looking for USA equivalent...")
        usa_country = next((c for c in countries if 'United States' in c.get('shortName', '') or c.get('isoa2') == 'US'), None)
        if usa_country:
            print(f"🇺🇸 Found USA: {usa_country}")
        else:
            print("❌ USA not found")
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
