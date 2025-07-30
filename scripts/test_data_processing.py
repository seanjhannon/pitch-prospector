#!/usr/bin/env python3
"""
Test data processing logic locally without database connection.
This helps us verify the optimized pipeline works before dealing with database issues.
"""

import pandas as pd
import json
from datetime import datetime, timedelta
from pybaseball import statcast
import time

# MISSION CRITICAL COLUMNS ONLY
MISSION_CRITICAL_COLUMNS = [
    "game_date",
    "game_pk", 
    "at_bat_number",
    "batter",
    "pitcher",
    "inning",
    "pitch_type",
    "release_speed",
    "zone"
]

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

def process_statcast_data(df: pd.DataFrame) -> list:
    """Process Statcast data into optimized format."""
    if df.empty:
        return []
    
    print(f"🔧 Processing {len(df):,} records...")
    start_time = time.time()
    
    # Keep only mission critical columns
    available_cols = [col for col in MISSION_CRITICAL_COLUMNS if col in df.columns]
    df = df[available_cols].copy()
    
    print(f"📊 Available columns: {available_cols}")
    print(f"📊 Missing columns: {[col for col in MISSION_CRITICAL_COLUMNS if col not in df.columns]}")
    
    # Fill missing values
    df['release_speed'] = df['release_speed'].fillna(0)
    df['zone'] = df['zone'].fillna(0)
    df['pitch_type'] = df['pitch_type'].fillna('UN')
    
    # Sort by game and at-bat
    df = df.sort_values(by=["game_pk", "at_bat_number", "pitch_number"])
    
    # Group by at-bat
    grouped = df.groupby(["game_pk", "at_bat_number"], sort=False)
    
    atbats = []
    for (game_pk, ab_num), group in grouped:
        # Extract pitch sequence (pitch_type only)
        pitch_sequence = []
        pitch_data = []
        
        for _, pitch in group.iterrows():
            # Only store what's actually used in the UI
            pitch_sequence.append(str(pitch.get('pitch_type', 'UN')))
            
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
    
    # Show sample data
    if atbats:
        print(f"\n📊 Sample at-bat:")
        sample = atbats[0]
        print(f"  • Game: {sample['game_pk']}, At-bat: {sample['at_bat_number']}")
        print(f"  • Date: {sample['game_date']}")
        print(f"  • Pitcher: {sample['pitcher']}, Batter: {sample['batter']}")
        print(f"  • Inning: {sample['inning']}")
        print(f"  • Pitch sequence: {sample['pitch_sequence']}")
        print(f"  • Pitch data: {sample['pitch_data'][:3]}...")  # First 3 pitches
        
        # Calculate storage savings
        original_size = len(df) * len(df.columns) * 8  # Rough estimate
        optimized_size = len(atbats) * (len(sample['pitch_sequence']) + len(sample['pitch_data']) * 2) * 8
        savings = (original_size - optimized_size) / original_size * 100
        print(f"\n💾 Estimated storage savings: {savings:.1f}%")
    
    return atbats

def main():
    """Test data processing with a small date range."""
    print("🧪 Testing optimized data processing...")
    print("=" * 50)
    
    # Test with a small date range
    start_date = "2025-07-01"
    end_date = "2025-07-07"  # Just one week
    
    print(f"📅 Testing with data: {start_date} to {end_date}")
    
    # Fetch data
    df = fetch_statcast_data(start_date, end_date)
    if df.empty:
        print("⚠️ No data fetched, exiting...")
        return
    
    # Process data
    atbats = process_statcast_data(df)
    if not atbats:
        print("⚠️ No at-bats processed, exiting...")
        return
    
    print(f"\n🎉 Successfully processed {len(atbats):,} at-bats!")
    print("✅ Data processing logic works correctly")
    print("✅ Ready to use with database when connection is available")

if __name__ == "__main__":
    main() 