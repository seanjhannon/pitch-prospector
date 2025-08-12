#!/usr/bin/env python3
"""
Test script for the Streamlit-compatible CockroachDB module.
Simulates Streamlit secrets to test the module functionality.
"""

import os
import base64
import sys

# Mock Streamlit secrets
class MockSecrets:
    def __init__(self):
        # Read the actual CA certificate and encode it
        cert_path = os.path.expanduser("~/.postgresql/root.crt")
        with open(cert_path, 'rb') as f:
            ca_cert_bytes = f.read()
        
        self.crdb = {
            "dsn": "postgresql://sean:_GS_iQHq4ZjjvwA4-VBqcQ@pitches-8229.jxf.gcp-us-west2.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full",
            "ca_cert_b64": base64.b64encode(ca_cert_bytes).decode('utf-8')
        }

# Mock Streamlit
class MockStreamlit:
    def __init__(self):
        self.secrets = MockSecrets()
    
    def error(self, message):
        print(f"❌ STREAMLIT ERROR: {message}")
    
    def stop(self):
        print("🛑 STREAMLIT STOPPED")
        sys.exit(1)

# Mock the cache_resource decorator
def mock_cache_resource(func):
    return func

# Mock logging
class MockLogger:
    def info(self, message):
        print(f"ℹ️  {message}")
    
    def error(self, message):
        print(f"❌ {message}")

# Patch the module before importing
import cockroach_streamlit_cloud
cockroach_streamlit_cloud.st = MockStreamlit()
cockroach_streamlit_cloud.logger = MockLogger()
cockroach_streamlit_cloud.st.cache_resource = mock_cache_resource

def test_streamlit_module():
    """Test the Streamlit-compatible module."""
    print("🔍 Testing Streamlit-compatible CockroachDB module...")
    
    try:
        # Test getting connection
        print("📡 Getting CockroachDB connection...")
        conn = cockroach_streamlit_cloud.get_cockroach_cloud_connection()
        print("✅ Connection object created successfully")
        
        # Test table interface
        print("📋 Testing table interface...")
        table = conn.table("information_schema.tables")
        print("✅ Table object created successfully")
        
        # Test basic query
        print("🔍 Testing basic query...")
        result = table.select("table_name").limit(3).execute()
        print(f"✅ Query executed successfully, got {result.count} results")
        
        # Show first few results
        for i, row in enumerate(result.data[:3]):
            print(f"   {i+1}. {row[0]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_streamlit_module()
    if success:
        print("\n🎉 Streamlit module test successful!")
        print("   Your CockroachDB module is ready for the app!")
    else:
        print("\n❌ Streamlit module test failed!")
        print("   We need to fix the module before proceeding") 