#!/usr/bin/env python3
"""
Script to create tables in Supabase PostgreSQL database
"""

import os
from dotenv import load_dotenv
import psycopg2
import sys

# Load environment variables
load_dotenv()

def get_supabase_connection():
    """Get connection to Supabase PostgreSQL database"""
    return psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=os.environ.get("SUPABASE_DB_PORT", 5432),
        dbname=os.environ["SUPABASE_DB_NAME"],
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"]
    )

def create_tables(conn):
    """Create tables in Supabase"""
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
        print("✅ Tables created successfully in Supabase!")

def main():
    """Main function"""
    print("Creating tables in Supabase...")
    
    try:
        # Connect to Supabase
        conn = get_supabase_connection()
        print("✅ Connected to Supabase")
        
        # Create tables
        create_tables(conn)
        
    except Exception as e:
        print(f"❌ Failed to create tables: {e}")
        sys.exit(1)
    finally:
        # Close connection
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main() 