#!/usr/bin/env python3
"""
Simple test to verify our CockroachDB module can be imported and used.
"""

def test_module_import():
    """Test that we can import and use our module."""
    print("🔍 Testing module import and basic functionality...")
    
    try:
        # Test 1: Can we import the module?
        print("📦 Testing module import...")
        import cockroach_streamlit_cloud
        print("✅ Module imported successfully")
        
        # Test 2: Can we access the main function?
        print("🔧 Testing function access...")
        get_conn = cockroach_streamlit_cloud.get_cockroach_cloud_connection
        print("✅ Main function accessible")
        
        # Test 3: Can we see the class definitions?
        print("🏗️  Testing class definitions...")
        CockroachDBCloudConnection = cockroach_streamlit_cloud.CockroachDBCloudConnection
        CockroachDBCloudTable = cockroach_streamlit_cloud.CockroachDBCloudTable
        print("✅ Classes defined successfully")
        
        # Test 4: Can we create a connection object (without actually connecting)?
        print("🔨 Testing object creation...")
        # We'll need to mock st.secrets for this to work
        class MockSecrets:
            def __getitem__(self, key):
                if key == "crdb":
                    return {
                        "dsn": "postgresql://test@localhost/test",
                        "ca_cert_b64": "dGVzdA=="  # "test" in base64
                    }
                raise KeyError(key)
        
        # Temporarily patch st.secrets
        original_st = cockroach_streamlit_cloud.st
        cockroach_streamlit_cloud.st = type('MockSt', (), {'secrets': MockSecrets(), 'error': lambda x: None, 'stop': lambda: None})()
        
        try:
            conn = get_conn()
            print("✅ Connection object created successfully")
            
            # Test 5: Can we create a table object?
            table = conn.table("test_table")
            print("✅ Table object created successfully")
            
            print("✅ All basic functionality tests passed!")
            return True
            
        finally:
            # Restore original st
            cockroach_streamlit_cloud.st = original_st
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_module_import()
    if success:
        print("\n🎉 Module test successful!")
        print("   Your CockroachDB module is ready!")
    else:
        print("\n❌ Module test failed!")
        print("   We need to fix the module") 