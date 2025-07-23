#!/usr/bin/env python3
"""
Insert pitch sequences for existing at-bats in smaller batches to avoid timeouts
"""
import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch
import sys
import ast

load_dotenv()

PARQUET_DATA_DIR = "pitch_prospector/data"
BATCH_SIZE = 100  # Much smaller batches to avoid timeouts

def get_supabase_connection():
    return psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=os.environ.get("SUPABASE_DB_PORT", 5432),
        dbname=os.environ["SUPABASE_DB_NAME"],
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"]
    )

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

def process_processed_parquet_file(file_path):
    """Process a pre-processed at-bat level Parquet file"""
    try:
        print(f"🔄 Processing: {os.path.basename(file_path)}")
        
        df = pd.read_parquet(file_path)
        
        atbat_rows = []
        for _, row in df.iterrows():
            try:
                pitch_sequence = row["pitch_sequence"]
                pitch_level_data = row["pitch_level_data"]
                
                if isinstance(pitch_sequence, str):
                    try:
                        pitch_sequence = ast.literal_eval(pitch_sequence)
                    except:
                        continue
                
                if isinstance(pitch_level_data, str):
                    try:
                        pitch_level_data = ast.literal_eval(pitch_level_data)
                    except:
                        continue
                
                atbat_data = {
                    "game_pk": row["game_pk"],
                    "at_bat_number": row["at_bat_number"],
                    "pitch_sequence": pitch_sequence,
                    "pitch_level_data": pitch_level_data
                }
                atbat_rows.append(atbat_data)
            except Exception as e:
                continue
        
        print(f"✅ Processed {len(atbat_rows)} at-bats from {os.path.basename(file_path)}")
        return atbat_rows
    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")
        return []

def insert_pitch_sequences_batch(conn, batch_rows):
    """Insert pitch sequences for a batch of atbats"""
    if not batch_rows:
        return 0
    
    pitch_rows = []
    
    with conn.cursor() as cur:
        for row in batch_rows:
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
    print("🚀 Starting pitch sequences insertion...")
    
    parquet_files = find_processed_parquet_files(PARQUET_DATA_DIR)
    if not parquet_files:
        print("❌ No pre-processed Parquet files found.")
        sys.exit(1)
    
    try:
        conn = get_supabase_connection()
        print("✅ Connected to Supabase")
        
        total_pitch_sequences = 0
        
        for file_path in parquet_files:
            atbat_rows = process_processed_parquet_file(file_path)
            if not atbat_rows:
                continue
            
            # Process in smaller batches
            for i in range(0, len(atbat_rows), BATCH_SIZE):
                batch = atbat_rows[i:i + BATCH_SIZE]
                inserted = insert_pitch_sequences_batch(conn, batch)
                total_pitch_sequences += inserted
                print(f"📥 Inserted batch {i//BATCH_SIZE + 1}: {inserted} pitch sequences")
        
        print(f"✅ Total pitch sequences inserted: {total_pitch_sequences}")
        print("🎉 Pitch sequences insertion completed!")
        
    except Exception as e:
        print(f"❌ Insertion failed: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main() 