#!/usr/bin/env python3
"""
Simple test for CockroachDB connection logic.
Tests the core functionality without Streamlit complexity.
"""

import os
import base64
import psycopg

def test_core_connection():
    """Test the core connection logic that our module will use."""
    print("🔍 Testing core CockroachDB connection logic...")
    
    try:
        # Read CA certificate and encode it (same logic our module will use)
        cert_path = os.path.expanduser("~/.postgresql/root.crt")
        with open(cert_path, 'rb') as f:
            ca_cert_bytes = f.read()
        
        ca_cert_b64 = base64.b64encode(ca_cert_bytes).decode('utf-8')
        print(f"✅ CA certificate encoded: {len(ca_cert_b64)} characters")
        
        # Base DSN (what would come from secrets)
        base_dsn = "postgresql://sean:5lyC6VuWmUnEGOV85DsChQ@pitches-8229.jxf.gcp-us-west2.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full"
        
        # Decode certificate back (what our module will do)
        ca_cert = base64.b64decode(ca_cert_b64).decode('utf-8')
        print(f"✅ CA certificate decoded: {len(ca_cert)} characters")
        
        # Build connection string (what our module will do)
        if "?" in base_dsn:
            dsn = f"{base_dsn}&sslrootcert={cert_path}"
        else:
            dsn = f"{base_dsn}?sslrootcert={cert_path}"
        
        print(f"🔗 Connection string built successfully")
        print(f"   Certificate path: {cert_path}")
        
        # Test the connection
        print("📡 Testing connection...")
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print(f"✅ Connection successful!")
                print(f"📊 Version: {version[0]}")
                
                # Test a simple query
                cur.execute("SELECT current_database(), current_user;")
                db_info = cur.fetchone()
                print(f"📁 Database: {db_info[0]}")
                print(f"👤 User: {db_info[1]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_core_connection()
    if success:
        print("\n🎉 Core connection test successful!")
        print("   The logic our module will use is working!")
    else:
        print("\n❌ Core connection test failed!")
        print("   We need to fix the connection logic") 