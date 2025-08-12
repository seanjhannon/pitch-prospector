import streamlit as st
import psycopg
import os

st.title("🐔 CockroachDB Connection Test (Secure)")

# Use Streamlit secrets instead of hardcoded credentials
try:
    # Get connection details from secrets
    host = st.secrets["cockroachdb"]["host"]
    port = st.secrets["cockroachdb"]["port"]
    database = st.secrets["cockroachdb"]["database"]
    user = st.secrets["cockroachdb"]["user"]
    password = st.secrets["cockroachdb"]["password"]
    
    dsn = f'postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require'
    
    st.success("🔒 **Secure:** Using Streamlit secrets (no hardcoded credentials)")
    
    if st.button("🔗 Test Connection"):
        try:
            with psycopg.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT COUNT(*) FROM atbats_optimized;')
                    count = cur.fetchone()[0]
                    st.success(f'✅ Connected! Found {count:,} rows')
                    
                    # Test a simple query
                    cur.execute('SELECT id, game_date FROM atbats_optimized LIMIT 3;')
                    rows = cur.fetchall()
                    st.write("📊 Sample data:")
                    for row in rows:
                        st.write(f"• ID: {row[0]}, Date: {row[1]}")
                        
        except Exception as e:
            st.error(f'❌ Connection failed: {e}')
            
    if st.button("🔍 Test Query"):
        try:
            with psycopg.connect(dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT game_date, COUNT(*) FROM atbats_optimized GROUP BY game_date ORDER BY game_date DESC LIMIT 5;')
                    rows = cur.fetchall()
                    st.write("📅 Recent games:")
                    for row in rows:
                        st.write(f"• {row[0]}: {row[1]:,} at-bats")
                        
        except Exception as e:
            st.error(f'❌ Query failed: {e}')
            
except KeyError as e:
    st.error(f"❌ Missing secret: {e}")
    st.write("**Configure in Streamlit Cloud:**")
    st.write("1. Go to App Settings → Secrets")
    st.write("2. Add this configuration:")
    st.code("""
[cockroachdb]
host = "pitches-8229.jxf.gcp-us-west2.cockroachlabs.cloud"
port = 26257
database = "defaultdb"
user = "sean"
password = "YOUR_NEW_PASSWORD"
""")
