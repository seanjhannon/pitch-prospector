#!/usr/bin/env python3
"""
DELETEME - One-time migration script
This script migrates pre-processed Parquet files to Supabase PostgreSQL.
DELETE THIS FILE AFTER MIGRATION IS COMPLETE.

Simple single-table migration - like the original SQLite approach
This creates one table with JSON fields for pitch sequences and data
"""
import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch
import sys
import json
import ast
import numpy as np
from datetime import datetime

load_dotenv()

PARQUET_DATA_DIR = "pitch_prospector/data"
BATCH_SIZE = 10000

def get_supabase_connection():
    return psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=os.environ.get("SUPABASE_DB_PORT", 5432),
        dbname=os.environ["SUPABASE_DB_NAME"],
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"]
    )

def create_simple_table(conn):
    """Create a single table for all at-bat data"""
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS atbats_simple;
            
            CREATE TABLE atbats_simple (
                id SERIAL PRIMARY KEY,
                game_pk BIGINT NOT NULL,
                at_bat_number INTEGER NOT NULL,
                game_date DATE NOT NULL,
                batter BIGINT NOT NULL,
                pitcher BIGINT NOT NULL,
                inning INTEGER NOT NULL,
                pitch_sequence_hash VARCHAR(40) NOT NULL,
                pitch_sequence JSONB,
                pitch_level_data JSONB,
                UNIQUE(game_pk, at_bat_number)
            );
        """)
        conn.commit()
        print("✅ Created simple atbats_simple table")

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

def process_processed_parquet_file(file_path):
    """Process a pre-processed at-bat level Parquet file"""
    try:
        print(f"🔄 Processing: {os.path.basename(file_path)}")
        
        df = pd.read_parquet(file_path)
        
        atbat_rows = []
        for _, row in df.iterrows():
            try:
                # Parse the complex columns
                pitch_sequence = parse_pitch_sequence(row["pitch_sequence"])
                pitch_level_data = parse_pitch_level_data(row["pitch_level_data"])
                
                if pitch_sequence is None or pitch_level_data is None:
                    continue
                
                # Convert to JSON for PostgreSQL
                pitch_sequence_json = json.dumps(pitch_sequence)
                pitch_level_data_json = json.dumps(pitch_level_data)
                
                atbat_data = {
                    "game_pk": row["game_pk"],
                    "at_bat_number": row["at_bat_number"],
                    "game_date": str(row["game_date"].date()) if hasattr(row["game_date"], "date") else str(row["game_date"]),
                    "batter": int(row["batter"]) if row["batter"] is not None else 0,
                    "pitcher": int(row["pitcher"]) if row["pitcher"] is not None else 0,
                    "inning": row["inning"],
                    "pitch_sequence_hash": row["pitch_sequence_hash"],
                    "pitch_sequence": pitch_sequence_json,
                    "pitch_level_data": pitch_level_data_json
                }
                atbat_rows.append(atbat_data)
            except Exception as e:
                continue
        
        print(f"✅ Processed {len(atbat_rows)} at-bats from {os.path.basename(file_path)}")
        return atbat_rows
    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")
        return []

def insert_atbats_batch(conn, atbat_rows):
    """Insert a batch of atbats into the simple table"""
    if not atbat_rows:
        return 0
    
    with conn.cursor() as cur:
        atbat_data = []
        for row in atbat_rows:
            atbat_data.append((
                row["game_pk"],
                row["at_bat_number"],
                row["game_date"],
                row["batter"],
                row["pitcher"],
                row["inning"],
                row["pitch_sequence_hash"],
                row["pitch_sequence"],
                row["pitch_level_data"]
            ))
        
        execute_batch(cur, """
            INSERT INTO atbats_simple (
                game_pk, at_bat_number, game_date, batter, pitcher, inning, 
                pitch_sequence_hash, pitch_sequence, pitch_level_data
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_pk, at_bat_number) DO NOTHING
        """, atbat_data)
        
        conn.commit()
        return len(atbat_data)

def main():
    print("🚀 Starting simple single-table migration...")
    print("⚠️  This will create a new table 'atbats_simple' with all data in one place")
    
    parquet_files = find_processed_parquet_files(PARQUET_DATA_DIR)
    if not parquet_files:
        print("❌ No pre-processed Parquet files found.")
        sys.exit(1)
    
    try:
        conn = get_supabase_connection()
        print("✅ Connected to Supabase")
        
        # Create the simple table
        create_simple_table(conn)
        
        # Process all Parquet files
        all_atbat_rows = []
        
        for file_path in parquet_files:
            atbat_rows = process_processed_parquet_file(file_path)
            if atbat_rows:
                all_atbat_rows.extend(atbat_rows)
        
        print(f"📊 Total atbats to insert: {len(all_atbat_rows)}")
        
        if not all_atbat_rows:
            print("❌ No atbats found in Parquet files")
            return
        
        # Insert in batches
        total_inserted = 0
        for i in range(0, len(all_atbat_rows), BATCH_SIZE):
            batch = all_atbat_rows[i:i + BATCH_SIZE]
            inserted = insert_atbats_batch(conn, batch)
            total_inserted += inserted
            print(f"📥 Inserted batch {i//BATCH_SIZE + 1}: {inserted} atbats")
        
        print(f"✅ Total atbats inserted: {total_inserted}")
        print("🎉 Migration completed successfully!")
        print("📋 Next: Update your app to use the 'atbats_simple' table")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main() 