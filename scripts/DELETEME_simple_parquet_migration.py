#!/usr/bin/env python3
"""
ONE-TIME SCRIPT: Simple migration of processed Parquet data to Supabase
This script directly reads the processed at-bat level Parquet files and inserts them into Supabase.
DELETE THIS SCRIPT AFTER USE.
"""

import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch
import sys
import ast

# Load environment variables
load_dotenv()

# Configuration
PARQUET_DATA_DIR = "pitch_prospector/data"  # Adjust this path to where your Parquet files are
BATCH_SIZE = 1000  # Process atbats in batches

def get_supabase_connection():
    """Get connection to Supabase PostgreSQL database"""
    return psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=os.environ.get("SUPABASE_DB_PORT", 5432),
        dbname=os.environ["SUPABASE_DB_NAME"],
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"]
    )

def find_processed_parquet_files(data_dir):
    """Find all pre-processed at-bat level Parquet files"""
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

def examine_parquet_structure(file_path):
    """Examine the structure of a Parquet file to understand the data format"""
    try:
        df = pd.read_parquet(file_path)
        print(f"📊 File: {os.path.basename(file_path)}")
        print(f"   Columns: {list(df.columns)}")
        print(f"   Shape: {df.shape}")
        if len(df) > 0:
            print(f"   Sample row: {df.iloc[0].to_dict()}")
        return df
    except Exception as e:
        print(f"❌ Error examining {file_path}: {e}")
        return None

def process_processed_parquet_file(file_path):
    """Process a pre-processed at-bat level Parquet file"""
    try:
        print(f"🔄 Processing: {os.path.basename(file_path)}")
        
        # Read the pre-processed data
        df = pd.read_parquet(file_path)
        
        # Convert to list of dictionaries
        atbat_rows = []
        for _, row in df.iterrows():
            try:
                # Handle different data formats
                pitch_sequence = row["pitch_sequence"]
                pitch_level_data = row["pitch_level_data"]
                
                # If they're strings, try to parse them
                if isinstance(pitch_sequence, str):
                    try:
                        pitch_sequence = ast.literal_eval(pitch_sequence)
                    except:
                        print(f"Warning: Could not parse pitch_sequence: {pitch_sequence[:100]}...")
                        continue
                
                if isinstance(pitch_level_data, str):
                    try:
                        pitch_level_data = ast.literal_eval(pitch_level_data)
                    except:
                        print(f"Warning: Could not parse pitch_level_data: {pitch_level_data[:100]}...")
                        continue
                
                atbat_data = {
                    "game_pk": row["game_pk"],
                    "at_bat_number": row["at_bat_number"],
                    "game_date": row["game_date"],
                    "batter": row["batter"],
                    "pitcher": row["pitcher"],
                    "inning": row["inning"],
                    "pitch_sequence_hash": row["pitch_sequence_hash"],
                    "pitch_sequence": pitch_sequence,
                    "pitch_level_data": pitch_level_data
                }
                atbat_rows.append(atbat_data)
            except Exception as e:
                print(f"Warning: Error processing row in {file_path}: {e}")
                continue
        
        print(f"✅ Processed {len(atbat_rows)} at-bats from {os.path.basename(file_path)}")
        return atbat_rows
    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")
        return []

def insert_atbats_batch(conn, atbat_rows):
    """Insert a batch of atbats into Supabase"""
    if not atbat_rows:
        return 0
    
    with conn.cursor() as cur:
        atbat_data = []
        for row in atbat_rows:
            # Ensure batter and pitcher are integers
            batter_id = int(row["batter"]) if row["batter"] is not None else 0
            pitcher_id = int(row["pitcher"]) if row["pitcher"] is not None else 0
            
            atbat_data.append((
                row["game_pk"],
                row["at_bat_number"],
                str(row["game_date"].date()) if hasattr(row["game_date"], "date") else str(row["game_date"]),
                batter_id,
                pitcher_id,
                row["inning"],
                row["pitch_sequence_hash"]
            ))
        
        execute_batch(cur, """
            INSERT INTO atbats (game_pk, at_bat_number, game_date, batter, pitcher, inning, pitch_sequence_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_pk, at_bat_number) DO NOTHING
        """, atbat_data)
        
        conn.commit()
        return len(atbat_data)

def insert_pitch_sequences_batch(conn, all_rows):
    """Insert pitch sequences for all atbats"""
    if not all_rows:
        return 0
    
    pitch_rows = []
    
    with conn.cursor() as cur:
        for row in all_rows:
            # Get the atbat_id from Supabase
            cur.execute(
                "SELECT id FROM atbats WHERE game_pk = %s AND at_bat_number = %s",
                (row["game_pk"], row["at_bat_number"])
            )
            result = cur.fetchone()
            if result:
                atbat_id = result[0]
                
                try:
                    for i, pitch in enumerate(row["pitch_sequence"]):
                        pitch_type, description = pitch
                        pitch_level_data = row["pitch_level_data"][i]
                        
                        pitch_rows.append((
                            atbat_id,
                            i,
                            pitch_type,
                            pitch_level_data.get("release_speed"),
                            pitch_level_data.get("zone"),
                            pitch_level_data.get("pfx_x"),
                            pitch_level_data.get("pfx_z"),
                            pitch_level_data.get("xc"),
                            pitch_level_data.get("yc"),
                            pitch_level_data.get("zc"),
                            pitch_level_data.get("vx0"),
                            pitch_level_data.get("vy0"),
                            pitch_level_data.get("vz0"),
                            pitch_level_data.get("ax"),
                            pitch_level_data.get("ay"),
                            pitch_level_data.get("az"),
                            pitch_level_data.get("break_y"),
                            pitch_level_data.get("break_angle"),
                            pitch_level_data.get("break_length"),
                            pitch_level_data.get("release_spin_rate"),
                            pitch_level_data.get("spin_direction"),
                            description
                        ))
                except Exception as e:
                    print(f"Warning: Error processing pitch sequences for atbat {row['game_pk']}-{row['at_bat_number']}: {e}")
                    continue
    
    if pitch_rows:
        with conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO pitch_sequences (
                    atbat_id, pitch_number, pitch_type, release_speed, zone,
                    pfx_x, pfx_z, xc, yc, zc, vx0, vy0, vz0, ax, ay, az,
                    break_y, break_angle, break_length, spin_rate, spin_direction, description
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (atbat_id, pitch_number) DO NOTHING
            """, pitch_rows)
            
            conn.commit()
    
    return len(pitch_rows)

def main():
    """Main migration function"""
    print("🚀 Starting simple Parquet to Supabase migration...")
    print("⚠️  WARNING: This is a one-time script. DELETE after use!")
    
    # Find pre-processed Parquet files
    parquet_files = find_processed_parquet_files(PARQUET_DATA_DIR)
    if not parquet_files:
        print("❌ No pre-processed Parquet files found. Please check the PARQUET_DATA_DIR path.")
        sys.exit(1)
    
    # Examine the first file to understand the structure
    print("\n🔍 Examining file structure...")
    examine_parquet_structure(parquet_files[0])
    
    try:
        # Connect to Supabase
        conn = get_supabase_connection()
        print("✅ Connected to Supabase")
        
        # Process all Parquet files
        all_atbat_rows = []
        
        # Process files one by one (no parallel processing to avoid issues)
        for file_path in parquet_files:
            atbat_rows = process_processed_parquet_file(file_path)
            if atbat_rows:
                all_atbat_rows.extend(atbat_rows)
        
        print(f"📊 Total atbats to insert: {len(all_atbat_rows)}")
        
        if not all_atbat_rows:
            print("❌ No atbats found in Parquet files")
            return
        
        # Insert atbats in batches
        total_inserted = 0
        for i in range(0, len(all_atbat_rows), BATCH_SIZE):
            batch = all_atbat_rows[i:i + BATCH_SIZE]
            inserted = insert_atbats_batch(conn, batch)
            total_inserted += inserted
            print(f"📥 Inserted batch {i//BATCH_SIZE + 1}: {inserted} atbats")
        
        print(f"✅ Total atbats inserted: {total_inserted}")
        
        # Insert pitch sequences
        print("🔄 Inserting pitch sequences...")
        pitch_count = insert_pitch_sequences_batch(conn, all_atbat_rows)
        print(f"✅ Total pitch sequences inserted: {pitch_count}")
        
        print("🎉 Migration completed successfully!")
        print("🗑️  Remember to DELETE this script after use!")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main() 