#!/usr/bin/env python3
"""
Test script to verify the population script works correctly.
"""

import streamlit as st
from st_supabase_connection import SupabaseConnection
import pandas as pd
from pybaseball import statcast
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize connection
conn = st.connection("supabase", type=SupabaseConnection)

def test_single_month():
    """Test fetching and processing a single month of data."""
    print("🧪 Testing single month data fetch...")
    
    # Test with a recent month that should have data
    test_start = datetime(2024, 9, 1)
    test_end = datetime(2024, 9, 30)
    
    print(f"🔍 Testing: {test_start.date()} to {test_end.date()}")
    
    try:
        # Fetch data
        df = statcast(start_dt=str(test_start.date()), end_dt=str(test_end.date()))
        
        if df.empty:
            print("  ⚠️  No data found for test period")
            return False
        
        print(f"  📊 Retrieved {len(df):,} pitches")
        
        # Test processing
        from scripts.populate_to_target_size import process_statcast_data
        atbats = process_statcast_data(df)
        
        print(f"  🔄 Processed {len(atbats):,} at-bats")
        
        if atbats:
            print(f"  ✅ Sample at-bat: {atbats[0]['game_date']} - {len(atbats[0]['pitch_sequence'])} pitches")
            return True
        else:
            print("  ❌ No valid at-bats found")
            return False
            
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def test_database_connection():
    """Test database connection and queries."""
    print("🧪 Testing database connection...")
    
    try:
        # Test count query
        result = conn.table("atbats_optimized").select("id", count="exact").execute()
        count = result.count if result.count is not None else 0
        print(f"  ✅ Database connection successful: {count:,} records")
        
        # Test date query
        result = conn.table("atbats_optimized").select("game_date").order("game_date", desc=False).limit(1).execute()
        if result.data:
            earliest_date = pd.to_datetime(result.data[0]['game_date'])
            print(f"  ✅ Earliest date: {earliest_date.date()}")
            return True
        else:
            print("  ❌ No date data found")
            return False
            
    except Exception as e:
        print(f"  ❌ Database error: {e}")
        return False

def main():
    """Main test function."""
    print("=" * 50)
    print("🧪 Pitch Prospector - Population Test Script")
    print("=" * 50)
    
    # Test database connection
    db_ok = test_database_connection()
    
    if db_ok:
        # Test data fetching
        data_ok = test_single_month()
        
        if data_ok:
            print("\n✅ All tests passed! Ready to run population script.")
        else:
            print("\n❌ Data fetching test failed.")
    else:
        print("\n❌ Database connection test failed.")
    
    print("\n" + "=" * 50)

if __name__ == "__main__":
    main() 