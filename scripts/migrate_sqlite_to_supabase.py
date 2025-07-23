#!/usr/bin/env python3
"""
Migration script to move data from local SQLite to Supabase PostgreSQL
"""

import os
import sqlite3
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch
import sys

# Load environment variables
load_dotenv()

# SQLite database path
SQLITE_DB_PATH = "pitch_prospector/data/pitchprospector.sqlite"

def get_sqlite_connection():
    """Get connection to local SQLite database"""
    return sqlite3.connect(SQLITE_DB_PATH)

def get_supabase_connection():
    """Get connection to Supabase PostgreSQL database"""
    return psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=os.environ.get("SUPABASE_DB_PORT", 5432),
        dbname=os.environ["SUPABASE_DB_NAME"],
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"]
    )

def create_tables_if_not_exist(conn):
    """Create tables in Supabase if they don't exist"""
    with conn.cursor() as cur:
        # Create atbats table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS atbats (
                id SERIAL PRIMARY KEY,
                game_pk BIGINT NOT NULL,
                at_bat_number INTEGER NOT NULL,
                game_date DATE NOT NULL,
                batter BIGINT NOT NULL,
                pitcher BIGINT NOT NULL,
                inning INTEGER NOT NULL,
                pitch_sequence_hash VARCHAR(40) NOT NULL,
                UNIQUE(game_pk, at_bat_number)
            )
        """)
        
        # Create pitch_sequences table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pitch_sequences (
                id SERIAL PRIMARY KEY,
                atbat_id INTEGER NOT NULL REFERENCES atbats(id) ON DELETE CASCADE,
                pitch_number INTEGER NOT NULL,
                pitch_type VARCHAR(10),
                release_speed DECIMAL(4,1),
                zone INTEGER,
                pfx_x DECIMAL(4,2),
                pfx_z DECIMAL(4,2),
                xc DECIMAL(4,2),
                yc DECIMAL(4,2),
                zc DECIMAL(4,2),
                vx0 DECIMAL(6,2),
                vy0 DECIMAL(6,2),
                vz0 DECIMAL(6,2),
                ax DECIMAL(6,2),
                ay DECIMAL(6,2),
                az DECIMAL(6,2),
                break_y DECIMAL(4,2),
                break_angle DECIMAL(4,2),
                break_length DECIMAL(4,2),
                spin_rate INTEGER,
                spin_direction INTEGER,
                description VARCHAR(100),
                UNIQUE(atbat_id, pitch_number)
            )
        """)
        
        conn.commit()

def migrate_atbats(sqlite_conn, supabase_conn):
    """Migrate atbats data from SQLite to Supabase"""
    print("Migrating atbats...")
    
    # Read from SQLite
    with sqlite_conn.cursor() as cur:
        cur.execute("SELECT * FROM atbats")
        atbats = cur.fetchall()
    
    if not atbats:
        print("No atbats found in SQLite database")
        return
    
    print(f"Found {len(atbats)} atbats to migrate")
    
    # Insert into Supabase
    with supabase_conn.cursor() as cur:
        # Clear existing data (optional - comment out if you want to append)
        cur.execute("DELETE FROM pitch_sequences")
        cur.execute("DELETE FROM atbats")
        
        # Insert atbats
        atbat_data = []
        for row in atbats:
            # Convert bytes to integers for player IDs
            batter_id = int.from_bytes(row[4], byteorder='little') if isinstance(row[4], bytes) else row[4]
            pitcher_id = int.from_bytes(row[5], byteorder='little') if isinstance(row[5], bytes) else row[5]
            
            atbat_data.append((
                row[1],  # game_pk
                row[2],  # at_bat_number
                row[3],  # game_date
                batter_id,  # batter (converted)
                pitcher_id,  # pitcher (converted)
                row[6],  # inning
                row[7]   # pitch_sequence_hash
            ))
        
        execute_batch(cur, """
            INSERT INTO atbats (game_pk, at_bat_number, game_date, batter, pitcher, inning, pitch_sequence_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_pk, at_bat_number) DO NOTHING
        """, atbat_data)
        
        supabase_conn.commit()
        print(f"Successfully migrated {len(atbat_data)} atbats")

def migrate_pitch_sequences(sqlite_conn, supabase_conn):
    """Migrate pitch_sequences data from SQLite to Supabase"""
    print("Migrating pitch sequences...")
    
    # Read from SQLite
    with sqlite_conn.cursor() as cur:
        cur.execute("SELECT * FROM pitch_sequences")
        pitch_sequences = cur.fetchall()
    
    if not pitch_sequences:
        print("No pitch sequences found in SQLite database")
        return
    
    print(f"Found {len(pitch_sequences)} pitch sequences to migrate")
    
    # Get atbat ID mapping from Supabase
    with supabase_conn.cursor() as cur:
        cur.execute("SELECT id, game_pk, at_bat_number FROM atbats")
        atbat_mapping = {(row[1], row[2]): row[0] for row in cur.fetchall()}
    
    # Insert into Supabase
    with supabase_conn.cursor() as cur:
        pitch_data = []
        for row in pitch_sequences:
            # Map SQLite atbat_id to Supabase atbat_id
            sqlite_atbat_id = row[1]
            
            # Get the corresponding atbat from SQLite to find game_pk and at_bat_number
            with sqlite_conn.cursor() as sqlite_cur:
                sqlite_cur.execute("SELECT game_pk, at_bat_number FROM atbats WHERE id = ?", (sqlite_atbat_id,))
                atbat_info = sqlite_cur.fetchone()
            
            if atbat_info:
                game_pk, at_bat_number = atbat_info
                supabase_atbat_id = atbat_mapping.get((game_pk, at_bat_number))
                
                if supabase_atbat_id:
                    pitch_data.append((
                        supabase_atbat_id,  # atbat_id (mapped)
                        row[2],  # pitch_number
                        row[3],  # pitch_type
                        row[4],  # release_speed
                        row[5],  # zone
                        row[6],  # pfx_x
                        row[7],  # pfx_z
                        row[8],  # xc
                        row[9],  # yc
                        row[10], # zc
                        row[11], # vx0
                        row[12], # vy0
                        row[13], # vz0
                        row[14], # ax
                        row[15], # ay
                        row[16], # az
                        row[17], # break_y
                        row[18], # break_angle
                        row[19], # break_length
                        row[20], # spin_rate
                        row[21], # spin_direction
                        row[22]  # description
                    ))
        
        if pitch_data:
            execute_batch(cur, """
                INSERT INTO pitch_sequences (
                    atbat_id, pitch_number, pitch_type, release_speed, zone,
                    pfx_x, pfx_z, xc, yc, zc, vx0, vy0, vz0, ax, ay, az,
                    break_y, break_angle, break_length, spin_rate, spin_direction, description
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (atbat_id, pitch_number) DO NOTHING
            """, pitch_data)
            
            supabase_conn.commit()
            print(f"Successfully migrated {len(pitch_data)} pitch sequences")
        else:
            print("No pitch sequences could be mapped to existing atbats")

def main():
    """Main migration function"""
    print("Starting migration from SQLite to Supabase...")
    
    # Check if SQLite database exists
    if not os.path.exists(SQLITE_DB_PATH):
        print(f"Error: SQLite database not found at {SQLITE_DB_PATH}")
        sys.exit(1)
    
    try:
        # Connect to both databases
        sqlite_conn = get_sqlite_connection()
        supabase_conn = get_supabase_connection()
        
        print("✅ Connected to both databases")
        
        # Create tables in Supabase
        create_tables_if_not_exist(supabase_conn)
        print("✅ Tables created/verified in Supabase")
        
        # Migrate data
        migrate_atbats(sqlite_conn, supabase_conn)
        migrate_pitch_sequences(sqlite_conn, supabase_conn)
        
        print("✅ Migration completed successfully!")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        sys.exit(1)
    finally:
        # Close connections
        if 'sqlite_conn' in locals():
            sqlite_conn.close()
        if 'supabase_conn' in locals():
            supabase_conn.close()

if __name__ == "__main__":
    main() 