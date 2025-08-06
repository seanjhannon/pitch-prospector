#!/usr/bin/env python3
"""
Script to check the date range of data in the Supabase database.
"""

import streamlit as st
from st_supabase_connection import SupabaseConnection
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize connection
conn = st.connection("supabase", type=SupabaseConnection)

def check_date_range():
    """Check the date range of data in the database."""
    print("🔍 Checking date range of data in Supabase database...")
    
    try:
        # Get earliest date
        print("📅 Getting earliest date...")
        result = conn.table("atbats_optimized").select("game_date").order("game_date", desc=False).limit(1).execute()
        if result.data:
            earliest_date = pd.to_datetime(result.data[0]['game_date'])
            print(f"  ✅ Earliest date: {earliest_date.date()}")
        else:
            print("  ❌ No data found")
            return
        
        # Get latest date
        print("📅 Getting latest date...")
        result = conn.table("atbats_optimized").select("game_date").order("game_date", desc=True).limit(1).execute()
        if result.data:
            latest_date = pd.to_datetime(result.data[0]['game_date'])
            print(f"  ✅ Latest date: {latest_date.date()}")
        else:
            print("  ❌ No data found")
            return
        
        # Get total count
        print("📊 Getting total record count...")
        result = conn.table("atbats_optimized").select("id", count="exact").execute()
        total_count = result.count if result.count is not None else 0
        print(f"  ✅ Total records: {total_count:,}")
        
        # Calculate date range
        date_range = (latest_date - earliest_date).days
        print(f"📈 Date range: {date_range} days")
        
        # Get some sample data by year
        print("📊 Getting sample data by year...")
        for year in range(earliest_date.year, latest_date.year + 1):
            result = conn.table("atbats_optimized").select("id", count="exact").gte("game_date", f"{year}-01-01").lt("game_date", f"{year+1}-01-01").execute()
            year_count = result.count if result.count is not None else 0
            if year_count > 0:
                print(f"  📅 {year}: {year_count:,} records")
        
        print("\n" + "="*60)
        print("📋 SUMMARY:")
        print(f"   Earliest date: {earliest_date.date()}")
        print(f"   Latest date: {latest_date.date()}")
        print(f"   Total records: {total_count:,}")
        print(f"   Date range: {date_range} days")
        print(f"   Years covered: {earliest_date.year} - {latest_date.year}")
        print("="*60)
        
    except Exception as e:
        print(f"❌ Error checking date range: {e}")

def check_recent_data():
    """Check recent data availability."""
    print("\n🔍 Checking recent data availability...")
    
    try:
        # Check last 7 days
        from datetime import timedelta
        today = datetime.today().date()
        week_ago = today - timedelta(days=7)
        
        result = conn.table("atbats_optimized").select("id", count="exact").gte("game_date", str(week_ago)).execute()
        recent_count = result.count if result.count is not None else 0
        print(f"  📊 Last 7 days: {recent_count:,} records")
        
        # Check last 30 days
        month_ago = today - timedelta(days=30)
        result = conn.table("atbats_optimized").select("id", count="exact").gte("game_date", str(month_ago)).execute()
        month_count = result.count if result.count is not None else 0
        print(f"  📊 Last 30 days: {month_count:,} records")
        
    except Exception as e:
        print(f"❌ Error checking recent data: {e}")

def main():
    """Main function."""
    print("=" * 60)
    print("🏟️  Pitch Prospector - Database Date Range Check")
    print("=" * 60)
    
    check_date_range()
    check_recent_data()
    
    print("\n" + "=" * 60)
    print("✅ Date range check completed!")

if __name__ == "__main__":
    main() 