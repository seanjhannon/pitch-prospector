#!/usr/bin/env python3
"""
Improved script to populate the database backwards from current data until it reaches 0.4GB.
This script works backwards in time to add historical data efficiently with better logging, duplicate avoidance, and timeout handling.
"""

import streamlit as st
from st_supabase_connection import SupabaseConnection
import pandas as pd
from pybaseball import statcast
from datetime import datetime, timedelta
import time
import sys
import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('population_log.txt'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

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
        
        logger.info(f"📊 Current database: {count:,} records")
        logger.info(f"📊 Estimated size: {estimated_size_bytes / (1024**3):.3f} GB")
        
        return count, estimated_size_bytes
    except Exception as e:
        logger.error(f"❌ Error getting database size: {e}")
        return 0, 0

def get_earliest_date():
    """Get the earliest date in the database."""
    try:
        result = conn.table("atbats_optimized").select("game_date").order("game_date", desc=False).limit(1).execute()
        if result.data:
            earliest_date = pd.to_datetime(result.data[0]['game_date'])
            logger.info(f"📅 Earliest date in database: {earliest_date.date()}")
            return earliest_date
        return None
    except Exception as e:
        logger.error(f"❌ Error getting earliest date: {e}")
        return None

def check_existing_data_for_month(start_date, end_date):
    """Check if we already have data for a specific month."""
    try:
        result = conn.table("atbats_optimized").select("id", count="exact").gte("game_date", str(start_date.date())).lte("game_date", str(end_date.date())).execute()
        count = result.count if result.count is not None else 0
        return count
    except Exception as e:
        logger.error(f"❌ Error checking existing data: {e}")
        return 0

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

def insert_atbats_with_retry(atbats, batch_size=50, max_retries=3):
    """Insert at-bats in small batches with retry logic."""
    if not atbats:
        return 0
    
    total_inserted = 0
    total_batches = (len(atbats) + batch_size - 1) // batch_size
    
    logger.info(f"  📦 Inserting {len(atbats):,} at-bats in {total_batches} batches of {batch_size}")
    
    for i in range(0, len(atbats), batch_size):
        batch = atbats[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        
        for attempt in range(max_retries):
            try:
                logger.info(f"    📦 Batch {batch_num}/{total_batches}: Inserting {len(batch)} at-bats (attempt {attempt + 1})")
                
                # Use upsert to handle duplicates
                result = conn.table("atbats_optimized").upsert(batch, count="exact").execute()
                inserted_count = result.count if result.count is not None else len(batch)
                
                logger.info(f"    ✅ Batch {batch_num}: Inserted {inserted_count} at-bats")
                total_inserted += inserted_count
                
                # Small delay between batches to avoid overwhelming the database
                time.sleep(0.5)
                break
                
            except Exception as e:
                logger.warning(f"    ⚠️  Batch {batch_num} attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"    ⏳ Retrying in 2 seconds...")
                    time.sleep(2)
                else:
                    logger.error(f"    ❌ Batch {batch_num} failed after {max_retries} attempts")
                    # Continue with next batch instead of failing completely
    
    logger.info(f"  ✅ Total inserted: {total_inserted:,} at-bats")
    return total_inserted

def fetch_and_insert_month(start_date, end_date):
    """Fetch and insert data for a specific month."""
    logger.info(f"🔍 Fetching data for {start_date.date()} to {end_date.date()}")
    
    # Check if we already have data for this month
    existing_count = check_existing_data_for_month(start_date, end_date)
    if existing_count > 0:
        logger.info(f"  ⚠️  Already have {existing_count:,} records for this month, skipping...")
        return 0
    
    try:
        # Fetch Statcast data - convert dates to strings
        logger.info(f"  📡 Fetching from Statcast API...")
        df = statcast(start_dt=str(start_date.date()), end_dt=str(end_date.date()))
        
        if df.empty:
            logger.warning(f"  ⚠️  No data found for {start_date.date()} to {end_date.date()}")
            return 0
        
        logger.info(f"  📊 Retrieved {len(df):,} pitches from API")
        
        # Process data
        logger.info(f"  🔄 Processing data...")
        atbats = process_statcast_data(df)
        
        if not atbats:
            logger.warning(f"  ⚠️  No valid at-bats found for {start_date.date()} to {end_date.date()}")
            return 0
        
        logger.info(f"  🔄 Processing {len(atbats):,} at-bats")
        
        # Insert data in small batches with retry logic
        inserted_count = insert_atbats_with_retry(atbats, batch_size=50)
        
        return inserted_count
        
    except Exception as e:
        logger.error(f"  ❌ Error processing {start_date.date()} to {end_date.date()}: {e}")
        return 0

def populate_backwards():
    """Populate database backwards until target size is reached."""
    logger.info("🚀 Starting backwards population to reach 0.4GB target...")
    
    # Get current state
    current_count, current_size = get_current_database_size()
    earliest_date = get_earliest_date()
    
    if earliest_date is None:
        logger.error("❌ Could not determine earliest date in database")
        return
    
    logger.info(f"🎯 Target size: {TARGET_SIZE_GB:.1f} GB ({TARGET_SIZE_BYTES:,} bytes)")
    logger.info(f"📊 Current size: {current_size / (1024**3):.3f} GB ({current_size:,} bytes)")
    
    if current_size >= TARGET_SIZE_BYTES:
        logger.info("✅ Database already at or above target size!")
        return
    
    # Calculate how much more data we need
    needed_bytes = TARGET_SIZE_BYTES - current_size
    estimated_records_needed = int(needed_bytes / 250)  # 250 bytes per record estimate
    
    logger.info(f"📈 Need approximately {estimated_records_needed:,} more records")
    
    # Start from the earliest date and work backwards
    current_date = earliest_date - timedelta(days=1)  # Start from day before earliest
    end_date = datetime(2015, 1, 1)  # Statcast data starts around 2015
    
    total_inserted = 0
    month_count = 0
    consecutive_empty_months = 0
    start_time = time.time()
    
    logger.info(f"🕐 Starting population at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    while current_date >= end_date and current_size < TARGET_SIZE_BYTES:
        # Process one month at a time
        month_start = current_date.replace(day=1)
        month_end = current_date
        
        logger.info(f"\n📅 Processing month {month_start.strftime('%B %Y')} ({month_count + 1})")
        
        inserted = fetch_and_insert_month(month_start, month_end)
        total_inserted += inserted
        month_count += 1
        
        # Track consecutive empty months
        if inserted == 0:
            consecutive_empty_months += 1
            logger.info(f"  📊 Month {month_start.strftime('%B %Y')}: No new data")
        else:
            consecutive_empty_months = 0
            logger.info(f"  📊 Month {month_start.strftime('%B %Y')}: Added {inserted:,} records")
        
        # Update current size estimate
        current_size = (current_count + total_inserted) * 250
        
        # Calculate progress and time estimates
        elapsed_time = time.time() - start_time
        progress_pct = (current_size / TARGET_SIZE_BYTES) * 100
        
        logger.info(f"📊 Progress: {current_size / (1024**3):.3f} GB / {TARGET_SIZE_GB:.1f} GB ({progress_pct:.1f}%)")
        logger.info(f"📈 Total inserted: {total_inserted:,} records")
        logger.info(f"⏱️  Elapsed time: {elapsed_time/60:.1f} minutes")
        
        # Move to previous month
        current_date = month_start - timedelta(days=1)
        
        # Add delay to avoid overwhelming the API
        logger.info(f"  ⏳ Waiting 2 seconds before next request...")
        time.sleep(2)
        
        # Safety checks
        if month_count > 120:  # 10 years of months
            logger.warning("⚠️  Reached safety limit of 10 years back")
            break
        
        if consecutive_empty_months >= 6:  # 6 consecutive empty months
            logger.warning("⚠️  Too many consecutive empty months, stopping")
            break
    
    # Final size check
    final_count, final_size = get_current_database_size()
    total_time = time.time() - start_time
    
    logger.info(f"\n🎉 Population completed!")
    logger.info(f"📊 Final database size: {final_size / (1024**3):.3f} GB")
    logger.info(f"📈 Total records: {final_count:,}")
    logger.info(f"📈 Records added: {total_inserted:,}")
    logger.info(f"📅 Months processed: {month_count}")
    logger.info(f"⏱️  Total time: {total_time/60:.1f} minutes")
    logger.info(f"📈 Records per minute: {total_inserted/(total_time/60):.1f}")

def main():
    """Main function."""
    logger.info("=" * 60)
    logger.info("🏟️  Pitch Prospector - Database Population Script v3")
    logger.info("=" * 60)
    
    try:
        populate_backwards()
    except KeyboardInterrupt:
        logger.warning("\n⚠️  Population interrupted by user")
    except Exception as e:
        logger.error(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ Script completed!")

if __name__ == "__main__":
    main() 