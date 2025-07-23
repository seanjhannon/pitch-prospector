#!/usr/bin/env python3
"""
DELETEME - Debug script for migration troubleshooting
This script was used to debug parsing issues during migration.
DELETE THIS FILE AFTER MIGRATION IS COMPLETE.

Test the fixed parsing logic
"""
import os
import pandas as pd
import numpy as np
import json
import ast
from datetime import datetime

PARQUET_DATA_DIR = "pitch_prospector/data"

def convert_numpy_to_python(obj):
    """Recursively convert numpy arrays and objects to Python native types"""
    if isinstance(obj, np.ndarray):
        # Convert to list first, then recursively convert any remaining numpy objects
        return convert_numpy_to_python(obj.tolist())
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, list):
        return [convert_numpy_to_python(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_numpy_to_python(value) for key, value in obj.items()}
    elif isinstance(obj, datetime):
        return obj.isoformat()
    else:
        return obj

def parse_pitch_sequence(pitch_sequence):
    """Parse pitch sequence from various formats"""
    if isinstance(pitch_sequence, str):
        try:
            return ast.literal_eval(pitch_sequence)
        except:
            return None
    
    # Convert any numpy objects to Python native types
    return convert_numpy_to_python(pitch_sequence)

def parse_pitch_level_data(pitch_level_data):
    """Parse pitch level data from various formats"""
    if isinstance(pitch_level_data, str):
        try:
            return ast.literal_eval(pitch_level_data)
        except:
            return None
    
    # Convert any numpy objects to Python native types
    return convert_numpy_to_python(pitch_level_data)

def test_parsing():
    """Test parsing on a single row"""
    # Find first Parquet file
    parquet_files = []
    for root, dirs, files in os.walk(PARQUET_DATA_DIR):
        for file in files:
            if file.endswith('.parquet') and 'atbat_pitch_sequence_index' in file:
                parquet_files.append(os.path.join(root, file))
    
    if not parquet_files:
        print("❌ No Parquet files found!")
        return
    
    file_path = parquet_files[0]
    print(f"Testing with: {os.path.basename(file_path)}")
    
    df = pd.read_parquet(file_path)
    if len(df) == 0:
        print("❌ File is empty!")
        return
    
    row = df.iloc[0]
    print(f"Row shape: {df.shape}")
    print(f"pitch_sequence type: {type(row['pitch_sequence'])}")
    print(f"pitch_level_data type: {type(row['pitch_level_data'])}")
    
    # Test parsing
    pitch_sequence = parse_pitch_sequence(row["pitch_sequence"])
    pitch_level_data = parse_pitch_level_data(row["pitch_level_data"])
    
    print(f"Parsed pitch_sequence type: {type(pitch_sequence)}")
    print(f"Parsed pitch_level_data type: {type(pitch_level_data)}")
    
    # Debug: Check what's in the parsed data
    if isinstance(pitch_sequence, list) and len(pitch_sequence) > 0:
        print(f"First pitch_sequence item: {pitch_sequence[0]} (type: {type(pitch_sequence[0])})")
        if isinstance(pitch_sequence[0], list) and len(pitch_sequence[0]) > 0:
            print(f"First pitch_sequence sub-item: {pitch_sequence[0][0]} (type: {type(pitch_sequence[0][0])})")
    
    if isinstance(pitch_level_data, list) and len(pitch_level_data) > 0:
        print(f"First pitch_level_data item: {pitch_level_data[0]} (type: {type(pitch_level_data[0])})")
        if isinstance(pitch_level_data[0], dict):
            first_key = list(pitch_level_data[0].keys())[0]
            first_value = pitch_level_data[0][first_key]
            print(f"First pitch_level_data value: {first_value} (type: {type(first_value)})")
    
    if pitch_sequence is not None and pitch_level_data is not None:
        try:
            pitch_sequence_json = json.dumps(pitch_sequence)
            pitch_level_data_json = json.dumps(pitch_level_data)
            print("✅ JSON conversion successful!")
            print(f"pitch_sequence length: {len(pitch_sequence)}")
            print(f"pitch_level_data length: {len(pitch_level_data)}")
            return True
        except Exception as e:
            print(f"❌ JSON conversion failed: {e}")
            # Try to identify the problematic item
            try:
                json.dumps(pitch_sequence)
                print("pitch_sequence JSON conversion works")
            except Exception as e1:
                print(f"pitch_sequence JSON error: {e1}")
            
            try:
                json.dumps(pitch_level_data)
                print("pitch_level_data JSON conversion works")
            except Exception as e2:
                print(f"pitch_level_data JSON error: {e2}")
            return False
    else:
        print("❌ Parsing failed!")
        return False

if __name__ == "__main__":
    test_parsing() 