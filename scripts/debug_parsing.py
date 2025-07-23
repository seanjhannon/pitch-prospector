#!/usr/bin/env python3
"""
DELETEME - Debug script for parsing troubleshooting
This script was used to debug parsing issues during migration.
DELETE THIS FILE AFTER MIGRATION IS COMPLETE.

Debug script to see exactly what's happening during parsing
"""
import os
import pandas as pd
import numpy as np
import json
import ast

PARQUET_DATA_DIR = "pitch_prospector/data"

def parse_pitch_sequence(pitch_sequence):
    """Parse pitch sequence from various formats"""
    print(f"    Parsing pitch_sequence: {type(pitch_sequence)}")
    
    if isinstance(pitch_sequence, str):
        try:
            result = ast.literal_eval(pitch_sequence)
            print(f"    String parsed successfully: {result}")
            return result
        except Exception as e:
            print(f"    String parsing failed: {e}")
            return None
    
    # Handle numpy arrays
    if isinstance(pitch_sequence, list):
        print(f"    Processing list with {len(pitch_sequence)} items")
        parsed_sequence = []
        for i, item in enumerate(pitch_sequence):
            print(f"      Item {i}: {type(item)} = {item}")
            if isinstance(item, np.ndarray):
                # Convert numpy array to list of strings
                converted = item.tolist()
                print(f"      Converted numpy array: {converted}")
                parsed_sequence.append(converted)
            else:
                parsed_sequence.append(item)
        print(f"    Final parsed sequence: {parsed_sequence}")
        return parsed_sequence
    
    print(f"    Unknown type, returning as-is: {pitch_sequence}")
    return pitch_sequence

def parse_pitch_level_data(pitch_level_data):
    """Parse pitch level data from various formats"""
    print(f"    Parsing pitch_level_data: {type(pitch_level_data)}")
    
    if isinstance(pitch_level_data, str):
        try:
            result = ast.literal_eval(pitch_level_data)
            print(f"    String parsed successfully")
            return result
        except Exception as e:
            print(f"    String parsing failed: {e}")
            return None
    
    # If it's already a list, return as is
    if isinstance(pitch_level_data, list):
        print(f"    Already a list with {len(pitch_level_data)} items")
        return pitch_level_data
    
    print(f"    Unknown type, returning as-is: {pitch_level_data}")
    return pitch_level_data

def debug_single_row(file_path, row_index=0):
    """Debug a single row from a Parquet file"""
    try:
        print(f"\n🔍 Debugging row {row_index} from {os.path.basename(file_path)}")
        
        df = pd.read_parquet(file_path)
        if len(df) == 0:
            print("❌ File is empty!")
            return
        
        row = df.iloc[row_index]
        print(f"Row data:")
        for col in df.columns:
            value = row[col]
            if isinstance(value, str) and len(value) > 100:
                print(f"  {col}: {value[:100]}... (truncated)")
            else:
                print(f"  {col}: {value}")
        
        print(f"\nParsing process:")
        
        # Try to parse pitch_sequence
        pitch_sequence = parse_pitch_sequence(row["pitch_sequence"])
        print(f"  pitch_sequence result: {pitch_sequence}")
        
        # Try to parse pitch_level_data
        pitch_level_data = parse_pitch_level_data(row["pitch_level_data"])
        print(f"  pitch_level_data result: {type(pitch_level_data)} with {len(pitch_level_data) if isinstance(pitch_level_data, list) else 'N/A'} items")
        
        # Check if both are valid
        if pitch_sequence is None or pitch_level_data is None:
            print("❌ One or both parsing failed!")
            return False
        
        # Try to convert to JSON
        try:
            pitch_sequence_json = json.dumps(pitch_sequence)
            pitch_level_data_json = json.dumps(pitch_level_data)
            print("✅ JSON conversion successful")
            return True
        except Exception as e:
            print(f"❌ JSON conversion failed: {e}")
            return False
            
    except Exception as e:
        print(f"❌ Error debugging row: {e}")
        return False

def main():
    print("🔍 Debugging parsing process...")
    
    # Find first Parquet file
    parquet_files = []
    for root, dirs, files in os.walk(PARQUET_DATA_DIR):
        for file in files:
            if file.endswith('.parquet') and 'atbat_pitch_sequence_index' in file:
                parquet_files.append(os.path.join(root, file))
    
    if not parquet_files:
        print("❌ No Parquet files found!")
        return
    
    # Debug first few rows from first file
    file_path = parquet_files[0]
    for i in range(3):  # Debug first 3 rows
        success = debug_single_row(file_path, i)
        if success:
            print(f"✅ Row {i} would be processed successfully")
        else:
            print(f"❌ Row {i} would be skipped")

if __name__ == "__main__":
    main() 