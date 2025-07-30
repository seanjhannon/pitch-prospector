#!/usr/bin/env python3
"""
Script to populate the database backwards from current data until it reaches 0.4GB.
This script works backwards in time to add historical data efficiently.
"""

import streamlit as st
from st_supabase_connection import SupabaseConnection
import pandas as pd
from pybaseball import statcast
from datetime import datetime, timedelta
import time
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize connection
conn = st.connection("supabase", type=SupabaseConnection)

# Target size in GB
TARGET_SIZE_GB = 0.4
TARGET_SIZE_BYTES = TARGET_SIZE_GB * 1024 * 1024 * 1024

# Mission critical columns for storage optimization
MISSION_CRITICAL_COLUMNS = [
    "game_pk", "at_bat_number", "game_date", "batter", "pitcher", 
    "inning", "pitch_type", "description", "release_speed", "zone"
]

def get_current_database_size():
    """Get current database size in bytes."""
    try:
        # Query to get table size
        result = conn.table("atbats_optimized").select("id", count="exact").execute()
        count = result.count if result.count is not None else 0
        
        # Estimate size based on average record size
        # Based on our optimized schema, each record is roughly 200-300 bytes
        estimated_size_bytes = count * 250  # Conservative estimate
        
        print(f"📊 Current database: {count:,} records")
        print(f"📊 Estimated size: {estimated_size_bytes / (1024**3):.3f} GB")
        
        return count, estimated_size_bytes
    except Exception as e:
        print(f"❌ Error getting database size: {e}")
        return 0, 0

def get_earliest_date():
    """Get the earliest date in the database."""
    try:
        result = conn.table("atbats_optimized").select("game_date").order("game_date", desc=False).limit(1).execute()
        if result.data:
            earliest_date = pd.to_datetime(result.data[0]['game_date'])
            print(f"📅 Earliest date in database: {earliest_date.date()}")
            return earliest_date
        return None
    except Exception as e:
        print(f"❌ Error getting earliest date: {e}")
        return None

def process_statcast_data(df):
    """Process Statcast data into optimized format."""
    if df.empty:
        return []
    
    atbats = []
    
    # Group by at-bat
    for (game_pk, at_bat_number), atbat_group in df.groupby(['game_pk', 'at_bat_number']):
        if atbat_group.empty:
            continue
            
        # Sort by pitch number to maintain sequence
        atbat_group = atbat_group.sort_values('pitch_number')
        
        # Extract pitch sequence and data
        pitch_sequence = []
        pitch_data = []
        
        for _, pitch in atbat_group.iterrows():
            pitch_type = str(pitch.get('pitch_type', 'UN'))
            description = str(pitch.get('description', 'unknown'))
            
            # Fill missing descriptions with reasonable defaults
            if description == 'nan' or description == 'unknown':
                if 'strike' in pitch_type.lower():
                    description = 'called_strike'
                elif 'ball' in pitch_type.lower():
                    description = 'ball'
                else:
                    description = 'hit_into_play'
            
            pitch_sequence.append([pitch_type, description])
            
            # Extract speed and zone
            speed = float(pitch.get('release_speed', 0)) if pd.notna(pitch.get('release_speed')) else 0
            zone = int(pitch.get('zone', 0)) if pd.notna(pitch.get('zone')) else 0
            pitch_data.append([speed, zone])
        
        # Create at-bat record
        first_pitch = atbat_group.iloc[0]
        atbat_record = {
            'game_pk': int(first_pitch['game_pk']),
            'at_bat_number': int(first_pitch['at_bat_number']),
            'game_date': str(first_pitch['game_date'].date()),
            'batter': int(first_pitch['batter']),
            'pitcher': int(first_pitch['pitcher']),
            'inning': int(first_pitch['inning']),
            'pitch_sequence': pitch_sequence,
            'pitch_data': pitch_data
        }
        
        atbats.append(atbat_record)
    
    return atbats

def insert_atbats(atbats, batch_size=100):
    """Insert at-bats in batches."""
    if not atbats:
        return 0
    
    try:
        # Use upsert to handle duplicates
        result = conn.table("atbats_optimized").upsert(atbats, count="exact").execute()
        inserted_count = result.count if result.count is not None else len(atbats)
        print(f"  ✅ Inserted {inserted_count} at-bats")
        return inserted_count
    except Exception as e:
        print(f"  ❌ Error inserting at-bats: {e}")
        return 0

def fetch_and_insert_month(start_date, end_date):
    """Fetch and insert data for a specific month."""
    print(f"🔍 Fetching data for {start_date.date()} to {end_date.date()}")
    
    try:
        # Fetch Statcast data - convert dates to strings
        df = statcast(start_dt=str(start_date.date()), end_dt=str(end_date.date()))
        
        if df.empty:
            print(f"  ⚠️  No data found for {start_date.date()} to {end_date.date()}")
            return 0
        
        print(f"  📊 Retrieved {len(df):,} pitches")
        
        # Process data
        atbats = process_statcast_data(df)
        
        if not atbats:
            print(f"  ⚠️  No valid at-bats found for {start_date.date()} to {end_date.date()}")
            return 0
        
        print(f"  🔄 Processing {len(atbats):,} at-bats")
        
        # Insert data
        inserted_count = insert_atbats(atbats)
        
        return inserted_count
        
    except Exception as e:
        print(f"  ❌ Error processing {start_date.date()} to {end_date.date()}: {e}")
        return 0

def populate_backwards():
    """Populate database backwards until target size is reached."""
    print("🚀 Starting backwards population to reach 0.4GB target...")
    
    # Get current state
    current_count, current_size = get_current_database_size()
    earliest_date = get_earliest_date()
    
    if earliest_date is None:
        print("❌ Could not determine earliest date in database")
        return
    
    print(f"🎯 Target size: {TARGET_SIZE_GB:.1f} GB ({TARGET_SIZE_BYTES:,} bytes)")
    print(f"📊 Current size: {current_size / (1024**3):.3f} GB ({current_size:,} bytes)")
    
    if current_size >= TARGET_SIZE_BYTES:
        print("✅ Database already at or above target size!")
        return
    
    # Calculate how much more data we need
    needed_bytes = TARGET_SIZE_BYTES - current_size
    estimated_records_needed = int(needed_bytes / 250)  # 250 bytes per record estimate
    
    print(f"📈 Need approximately {estimated_records_needed:,} more records")
    
    # Start from the earliest date and work backwards
    current_date = earliest_date - timedelta(days=1)  # Start from day before earliest
    end_date = datetime(2015, 1, 1)  # Statcast data starts around 2015
    
    total_inserted = 0
    month_count = 0
    consecutive_empty_months = 0
    
    while current_date >= end_date and current_size < TARGET_SIZE_BYTES:
        # Process one month at a time
        month_start = current_date.replace(day=1)
        month_end = current_date
        
        print(f"\n📅 Processing month {month_start.strftime('%B %Y')}")
        
        inserted = fetch_and_insert_month(month_start, month_end)
        total_inserted += inserted
        month_count += 1
        
        # Track consecutive empty months
        if inserted == 0:
            consecutive_empty_months += 1
        else:
            consecutive_empty_months = 0
        
        # Update current size estimate
        current_size = (current_count + total_inserted) * 250
        
        print(f"📊 Progress: {current_size / (1024**3):.3f} GB / {TARGET_SIZE_GB:.1f} GB")
        print(f"📈 Total inserted: {total_inserted:,} records")
        
        # Move to previous month
        current_date = month_start - timedelta(days=1)
        
        # Add delay to avoid overwhelming the API
        time.sleep(1)
        
        # Safety checks
        if month_count > 120:  # 10 years of months
            print("⚠️  Reached safety limit of 10 years back")
            break
        
        if consecutive_empty_months >= 6:  # 6 consecutive empty months
            print("⚠️  Too many consecutive empty months, stopping")
            break
    
    # Final size check
    final_count, final_size = get_current_database_size()
    
    print(f"\n🎉 Population completed!")
    print(f"📊 Final database size: {final_size / (1024**3):.3f} GB")
    print(f"📈 Total records: {final_count:,}")
    print(f"📈 Records added: {total_inserted:,}")
    print(f"📅 Months processed: {month_count}")

def main():
    """Main function."""
    print("=" * 60)
    print("🏟️  Pitch Prospector - Database Population Script")
    print("=" * 60)
    
    try:
        populate_backwards()
    except KeyboardInterrupt:
        print("\n⚠️  Population interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("✅ Script completed!")

if __name__ == "__main__":
    main() 