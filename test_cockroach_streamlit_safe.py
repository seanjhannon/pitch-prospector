import streamlit as st
import psycopg
import os

st.title("🐔 CockroachDB Connection Test (Safe)")

# Use system trusted roots instead of custom certificate
# This fixes the "certificate file not found" error in Streamlit Cloud
dsn = 'postgresql://sean:_GS_iQHq4ZjjvwA4-VBqcQ@pitches-8229.jxf.gcp-us-west2.cockroachlabs.cloud:26257/defaultdb?sslmode=require'

st.write("🔧 **Authentication Fix:** Using `sslmode=require` with system certificates")
st.write("📁 **No local certificate file needed** - works in Streamlit Cloud")

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
        st.write("🔍 **Debug info:** This will help us identify the next issue")

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
