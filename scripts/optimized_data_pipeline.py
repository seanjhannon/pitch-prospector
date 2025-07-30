#!/usr/bin/env python3
"""
Optimized data pipeline for Pitch Prospector using Streamlit Supabase connection.
Only stores mission-critical columns to minimize storage usage.
"""

import os
import pandas as pd
import hashlib
import json
from datetime import datetime, timedelta
from pybaseball import statcast
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import time
import tqdm
import streamlit as st
from st_supabase_connection import SupabaseConnection

load_dotenv()

# Initialize Supabase connection
conn = st.connection("supabase", type=SupabaseConnection)

# MISSION CRITICAL COLUMNS ONLY
# These are the only columns actually used in the UI
MISSION_CRITICAL_COLUMNS = [
    # Core at-bat identification
    "game_date",
    "game_pk", 
    "at_bat_number",
    "batter",
    "pitcher",
    "inning",
    
    # Pitch sequence data (used in UI display)
    "pitch_type",
    "description",  # Added back - contains the pitch outcome
    "release_speed",
    "zone"
]

def create_optimized_table():
    """Create the optimized table using Supabase connection."""
    print("🔧 Creating optimized table...")
    
    try:
        # Use Supabase connection to execute DDL
        # Note: This requires admin privileges or we need to use direct SQL
        # For now, we'll create the table structure and let the app handle it
        print("⚠️ Table creation requires admin privileges. Please create the table manually:")
        print("""
        CREATE TABLE atbats_optimized (
            id SERIAL PRIMARY KEY,
            game_pk BIGINT NOT NULL,
            at_bat_number INTEGER NOT NULL,
            game_date DATE NOT NULL,
            batter BIGINT NOT NULL,
            pitcher BIGINT NOT NULL,
            inning INTEGER NOT NULL,
            pitch_sequence JSONB NOT NULL,
            pitch_data JSONB NOT NULL,
            UNIQUE(game_pk, at_bat_number)
        );
        
        CREATE INDEX idx_opt_game_date ON atbats_optimized(game_date);
        CREATE INDEX idx_opt_pitch_sequence ON atbats_optimized USING GIN(pitch_sequence);
        CREATE INDEX idx_opt_date_sequence ON atbats_optimized(game_date, pitch_sequence);
        CREATE INDEX idx_opt_pitcher ON atbats_optimized(pitcher);
        CREATE INDEX idx_opt_batter ON atbats_optimized(batter);
        """)
        
        print("✅ Table structure defined for atbats_optimized")
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        print("Please create the table manually using the SQL above")

def fetch_statcast_data(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch data from Statcast API."""
    print(f"📥 Fetching Statcast data from {start_date} to {end_date}...")
    start_time = time.time()
    
    try:
        df = statcast(start_date, end_date)
        duration = time.time() - start_time
        print(f"✅ Fetched {len(df):,} records in {duration:.2f}s")
        return df
    except Exception as e:
        print(f"❌ Error fetching data: {e}")
        return pd.DataFrame()

def process_statcast_data(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Process Statcast data into optimized format."""
    if df.empty:
        return []
    
    print(f"🔧 Processing {len(df):,} records...")
    start_time = time.time()
    
    # Keep only mission critical columns
    available_cols = [col for col in MISSION_CRITICAL_COLUMNS if col in df.columns]
    df = df[available_cols].copy()
    
    # Fill missing values
    df['release_speed'] = df['release_speed'].fillna(0)
    df['zone'] = df['zone'].fillna(0)
    df['pitch_type'] = df['pitch_type'].fillna('UN')
    df['description'] = df['description'].fillna('unknown')  # Fill missing descriptions
    
    # Sort by game and at-bat
    df = df.sort_values(by=["game_pk", "at_bat_number"])
    
    # Group by at-bat
    grouped = df.groupby(["game_pk", "at_bat_number"], sort=False)
    
    atbats = []
    for (game_pk, ab_num), group in grouped:
        # Extract pitch sequence as tuples (pitch_type, outcome)
        pitch_sequence = []
        pitch_data = []
        
        for _, pitch in group.iterrows():
            # Store as tuple (pitch_type, outcome) for proper sequence matching
            pitch_sequence.append([
                str(pitch.get('pitch_type', 'UN')),
                str(pitch.get('description', 'unknown'))
            ])
            
            pitch_data.append([
                float(pitch.get('release_speed', 0)),
                int(pitch.get('zone', 0))
            ])
        
        atbat = {
            "game_pk": int(game_pk),
            "at_bat_number": int(ab_num),
            "game_date": str(pd.to_datetime(group.iloc[0]["game_date"]).date()),
            "batter": int(group.iloc[0]["batter"]),
            "pitcher": int(group.iloc[0]["pitcher"]),
            "inning": int(group.iloc[0]["inning"]),
            "pitch_sequence": pitch_sequence,
            "pitch_data": pitch_data
        }
        atbats.append(atbat)
    
    duration = time.time() - start_time
    print(f"✅ Processed {len(atbats):,} at-bats in {duration:.2f}s")
    return atbats

def insert_atbats(atbats: List[Dict[str, Any]]):
    """Insert at-bats into the optimized table using Supabase connection."""
    if not atbats:
        return
    
    print(f"💾 Inserting {len(atbats):,} at-bats...")
    start_time = time.time()
    
    try:
        # Use Supabase connection to insert data in batches
        batch_size = 100  # Smaller batches to avoid connection issues
        total_inserted = 0
        
        for i in range(0, len(atbats), batch_size):
            batch = atbats[i:i + batch_size]
            batch_data = []
            
            for atbat in batch:
                data = {
                    "game_pk": atbat["game_pk"],
                    "at_bat_number": atbat["at_bat_number"],
                    "game_date": atbat["game_date"],
                    "batter": atbat["batter"],
                    "pitcher": atbat["pitcher"],
                    "inning": atbat["inning"],
                    "pitch_sequence": atbat["pitch_sequence"],
                    "pitch_data": atbat["pitch_data"]
                }
                batch_data.append(data)
            
            # Use upsert to handle conflicts
            result = conn.table("atbats_optimized").upsert(batch_data, on_conflict="game_pk,at_bat_number").execute()
            
            if result.data:
                total_inserted += len(result.data)
                print(f"  ✅ Inserted batch {i//batch_size + 1}/{(len(atbats) + batch_size - 1)//batch_size} ({len(result.data)} records)")
            else:
                print(f"  ⚠️ Batch {i//batch_size + 1} failed to insert")
        
        duration = time.time() - start_time
        print(f"✅ Inserted {total_inserted:,} at-bats in {duration:.2f}s")
        
    except Exception as e:
        print(f"❌ Error inserting at-bats: {e}")

def fetch_and_process_range(start_date: str, end_date: str):
    """Fetch and process data for a date range."""
    print(f"\n🔄 Processing range: {start_date} to {end_date}")
    
    # Fetch data
    df = fetch_statcast_data(start_date, end_date)
    if df.empty:
        print("⚠️ No data fetched, skipping...")
        return
    
    # Process data
    atbats = process_statcast_data(df)
    if not atbats:
        print("⚠️ No at-bats processed, skipping...")
        return
    
    # Insert data
    insert_atbats(atbats)

def analyze_storage_usage():
    """Analyze storage usage of the optimized table using Supabase connection."""
    print("\n📊 Analyzing storage usage...")
    
    try:
        # Use Supabase connection to query data
        result = conn.table("atbats_optimized").select("*", count="exact").execute()
        
        if result.count is not None:
            record_count = result.count
            print(f"📊 Optimized table stats:")
            print(f"  • Records: {record_count:,}")
            print(f"  • Note: Detailed storage analysis requires admin privileges")
        else:
            print("❌ Could not get record count")
            
    except Exception as e:
        print(f"❌ Error analyzing storage: {e}")

def test_queries():
    """Test query performance on optimized table using Supabase connection."""
    print("\n🔍 Testing query performance...")
    
    try:
        # Test 1: Date range query
        print("📊 Testing date range query...")
        start_time = time.time()
        result = conn.table("atbats_optimized").select("*", count="exact").gte("game_date", "2025-01-01").lte("game_date", "2025-07-25").execute()
        count = result.count if result.count is not None else 0
        duration = time.time() - start_time
        print(f"  • Found {count:,} records in {duration:.3f}s")
        
        # Test 2: JSONB sequence query (with outcomes) - fixed JSON syntax
        print("📊 Testing JSONB sequence query with outcomes...")
        start_time = time.time()
        # Use a simpler test sequence that should exist
        test_sequence = [["FF", "called_strike"], ["FF", "ball"]]
        result = conn.table("atbats_optimized").select("*", count="exact").eq("pitch_sequence", test_sequence).execute()
        count = result.count if result.count is not None else 0
        duration = time.time() - start_time
        print(f"  • Found {count:,} records in {duration:.3f}s")
        
        # Test 3: Combined query
        print("📊 Testing combined date and sequence query...")
        start_time = time.time()
        result = conn.table("atbats_optimized").select("*", count="exact").gte("game_date", "2025-01-01").lte("game_date", "2025-07-25").eq("pitch_sequence", test_sequence).execute()
        count = result.count if result.count is not None else 0
        duration = time.time() - start_time
        print(f"  • Found {count:,} records in {duration:.3f}s")
        
        # Test 4: Simple pitch type search (without outcomes)
        print("📊 Testing simple pitch type search...")
        start_time = time.time()
        # Search for at-bats that start with a four-seam fastball
        result = conn.table("atbats_optimized").select("*", count="exact").execute()
        if result.data:
            # Filter in Python for demonstration
            matching_atbats = []
            for atbat in result.data:
                if atbat.get('pitch_sequence') and len(atbat['pitch_sequence']) > 0:
                    first_pitch = atbat['pitch_sequence'][0]
                    if isinstance(first_pitch, list) and len(first_pitch) > 0 and first_pitch[0] == "FF":
                        matching_atbats.append(atbat)
            count = len(matching_atbats)
        else:
            count = 0
        duration = time.time() - start_time
        print(f"  • Found {count:,} at-bats starting with FF in {duration:.3f}s")
        
    except Exception as e:
        print(f"❌ Error testing queries: {e}")
        print(f"  Error details: {str(e)}")

def main():
    """Main pipeline execution."""
    print("🚀 Starting optimized data pipeline with Streamlit Supabase connection...")
    print("=" * 60)
    
    # Step 1: Create optimized table (or provide instructions)
    create_optimized_table()
    print()
    
    # Step 2: Fetch and process 2025 data only (test)
    start_date = "2025-03-01"  # MLB season start
    end_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"📅 Testing with 2025 data: {start_date} to {end_date}")
    fetch_and_process_range(start_date, end_date)
    print()
    
    # Step 3: Analyze storage usage
    analyze_storage_usage()
    print()
    
    # Step 4: Test queries
    test_queries()
    print()
    
    print("=" * 60)
    print("🎉 Optimized data pipeline with Streamlit Supabase connection completed!")
    print("\n💡 Benefits:")
    print("  • Uses official Streamlit Supabase connection")
    print("  • Proper pitch outcomes included in sequences")
    print("  • Optimized storage with essential data only")
    print("\n📋 Next steps:")
    print("  • Ensure atbats_optimized table exists with proper schema")
    print("  • Test the application with new data structure")
    print("  • If successful, scale to full historical data")

if __name__ == "__main__":
    main() 