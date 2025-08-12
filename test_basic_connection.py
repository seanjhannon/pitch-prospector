#!/usr/bin/env python3
"""
Simple test script for CockroachDB connection.
This doesn't require Streamlit, just tests the basic connection.
"""

import os
import base64
import psycopg

def test_basic_connection():
    """Test basic connection to CockroachDB."""
    print("🔍 Testing basic CockroachDB connection...")
    
    try:
        # Hardcoded connection details for testing
        base_dsn = "postgresql://sean:5lyC6VuWmUnEGOV85DsChQ@pitches-8229.jxf.gcp-us-west2.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full"
        
        # Read CA certificate from file
        cert_path = os.path.expanduser("~/.postgresql/root.crt")
        if not os.path.exists(cert_path):
            print(f"❌ CA certificate not found at: {cert_path}")
            return False
        
        with open(cert_path, 'r') as f:
            ca_cert = f.read()
        
        # Build DSN with certificate
        dsn = f"{base_dsn}&sslrootcert={cert_path}"
        
        print(f"🔗 Connecting to CockroachDB...")
        print(f"   Host: pitches-8229.jxf.gcp-us-west2.cockroachlabs.cloud")
        print(f"   Database: defaultdb")
        print(f"   User: sean")
        print(f"   SSL: verify-full")
        print(f"   Certificate: {cert_path}")
        
        # Test connection
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                print(f"✅ Successfully connected to CockroachDB!")
                print(f"📊 Version: {version[0]}")
                
                # Test basic query
                cur.execute("SELECT current_database(), current_user;")
                db_info = cur.fetchone()
                print(f"📁 Database: {db_info[0]}")
                print(f"👤 User: {db_info[1]}")
                
                return True
                
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    success = test_basic_connection()
    if success:
        print("\n🎉 Basic connection test successful!")
    else:
        print("\n❌ Basic connection test failed!")
        print("   Check your CA certificate and connection details") 