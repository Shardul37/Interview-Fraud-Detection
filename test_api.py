import requests
import json
from pathlib import Path

# API base URL
BASE_URL = "http://127.0.0.1:8000"
API_URL = f"{BASE_URL}/api/v1"

def test_api():
    """Test the API endpoints"""
    
    print("🧪 Testing AI Fraud Detection API\n")
    
    # Test 1: Health check
    print("1. Testing health check...")
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.json()}\n")
    except Exception as e:
        print(f"   Error: {e}\n")
    
    # Test 2: Service status
    print("2. Testing service status...")
    try:
        response = requests.get(f"{API_URL}/status")
        print(f"   Status: {response.status_code}")
        print(f"   Response: {json.dumps(response.json(), indent=2)}\n")
    except Exception as e:
        print(f"   Error: {e}\n")
    
    # Test 3: List available interviews
    print("3. Testing list interviews...")
    try:
        response = requests.get(f"{API_URL}/list-interviews")
        print(f"   Status: {response.status_code}")
        print(f"   Response: {json.dumps(response.json(), indent=2)}\n")
    except Exception as e:
        print(f"   Error: {e}\n")
    
    # Test 4: Process sample interview
    print("4. Testing sample interview processing...")
    try:
        response = requests.post(
            f"{API_URL}/process-sample",
            params={
                "interview_id": "test_sample",
                "save_results": True
            }
        )
        print(f"   Status: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            print(f"   Success: {result['success']}")
            print(f"   Interview ID: {result['interview_id']}")
            print(f"   Final Verdict: {result['result']['final_verdict']}")
            print(f"   Cheating Segments: {result['result']['cheating_segments']}")
        else:
            print(f"   Error: {response.json()}")
    except Exception as e:
        print(f"   Error: {e}\n")

if __name__ == "__main__":
    test_api()
