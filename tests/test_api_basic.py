#!/usr/bin/env python3
"""
Basic test to see if YouTube API key works at all
"""

import requests
import json

def test_basic_api():
    """Test basic YouTube API functionality"""
    
    api_key = "AIzaSyCXqi2fOvS-pdqCL_PX3HTS7Fd9M4Seq8k"
    
    print("🔍 Testing basic YouTube API functionality...")
    print("=" * 50)
    
    # Test 1: Simple channels.list with a known channel ID
    print("\n📺 Test 1: channels.list with known channel ID")
    try:
        url = "https://www.googleapis.com/youtube/v3/channels"
        params = {
            "part": "snippet",
            "id": "UC_x5XG1OV2P6uZZ5FSM9Ttw",  # Google Developers channel
            "key": api_key
        }
        
        print(f"   📡 URL: {url}")
        print(f"   📊 Params: {params}")
        
        response = requests.get(url, params=params, timeout=15)
        print(f"   📈 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            items = data.get("items", [])
            if items:
                snippet = items[0].get("snippet", {})
                print(f"   ✅ Success! Channel: {snippet.get('title', 'N/A')}")
                print(f"   📝 Description: {snippet.get('description', 'N/A')[:100]}...")
            else:
                print(f"   ❌ No items returned")
        else:
            print(f"   ❌ Error: {response.text}")
            
    except Exception as e:
        print(f"   💥 Exception: {e}")
    
    # Test 2: Check API quota
    print("\n📊 Test 2: Check API quota")
    try:
        url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "part": "snippet",
            "q": "test",
            "maxResults": 1,
            "key": api_key
        }
        
        print(f"   📡 URL: {url}")
        print(f"   📊 Params: {params}")
        
        response = requests.get(url, params=params, timeout=15)
        print(f"   📈 Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print(f"   ✅ Search API works!")
        elif response.status_code == 403:
            print(f"   ❌ 403 Forbidden - check API key permissions")
            print(f"   📋 Response: {response.text}")
        else:
            print(f"   ❌ Unexpected status: {response.text}")
            
    except Exception as e:
        print(f"   💥 Exception: {e}")

if __name__ == "__main__":
    test_basic_api()
