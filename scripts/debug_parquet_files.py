#!/usr/bin/env python3
"""
DELETEME - Debug script for examining Parquet file structure
This script was used to examine Parquet file structure during migration.
DELETE THIS FILE AFTER MIGRATION IS COMPLETE.

Debug script to examine Parquet file structure and content
"""
import os
import pandas as pd
import sys

PARQUET_DATA_DIR = "pitch_prospector/data"

def find_processed_parquet_files(data_dir):
    if not os.path.exists(data_dir):
        print(f"❌ Data directory not found: {data_dir}")
        return []
    
    parquet_files = []
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.parquet') and 'atbat_pitch_sequence_index' in file:
                parquet_files.append(os.path.join(root, file))
    
    print(f"📁 Found {len(parquet_files)} pre-processed Parquet files")
    return sorted(parquet_files)

def examine_parquet_file(file_path):
    """Examine a single Parquet file in detail"""
    try:
        print(f"\n🔍 Examining: {os.path.basename(file_path)}")
        
        # Read the file
        df = pd.read_parquet(file_path)
        
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
        
        if len(df) > 0:
            print(f"   First row data:")
            first_row = df.iloc[0]
            for col in df.columns:
                value = first_row[col]
                if isinstance(value, str) and len(value) > 100:
                    print(f"     {col}: {value[:100]}... (truncated)")
                else:
                    print(f"     {col}: {value}")
            
            # Check for specific columns we need
            required_cols = ["game_pk", "at_bat_number", "pitch_sequence", "pitch_level_data"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                print(f"   ❌ Missing required columns: {missing_cols}")
            else:
                print(f"   ✅ All required columns present")
                
                # Check data types
                print(f"   Data types:")
                for col in required_cols:
                    print(f"     {col}: {df[col].dtype}")
                
                # Check for null values
                print(f"   Null counts:")
                for col in required_cols:
                    null_count = df[col].isnull().sum()
                    print(f"     {col}: {null_count} nulls")
        else:
            print(f"   ❌ File is empty!")
            
    except Exception as e:
        print(f"   ❌ Error examining file: {e}")

def main():
    print("🔍 Debugging Parquet files...")
    
    parquet_files = find_processed_parquet_files(PARQUET_DATA_DIR)
    if not parquet_files:
        print("❌ No Parquet files found!")
        return
    
    # Examine first few files in detail
    for file_path in parquet_files[:3]:  # Just examine first 3 files
        examine_parquet_file(file_path)

if __name__ == "__main__":
    main() 