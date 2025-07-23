#!/usr/bin/env python3
"""
Check migration status - see how many at-bats and pitch sequences are in Supabase
"""
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def get_supabase_connection():
    return psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=os.environ.get("SUPABASE_DB_PORT", 5432),
        dbname=os.environ["SUPABASE_DB_NAME"],
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"]
    )

def check_migration_status():
    try:
        conn = get_supabase_connection()
        with conn.cursor() as cur:
            # Check atbats count
            cur.execute("SELECT COUNT(*) FROM atbats")
            atbats_count = cur.fetchone()[0]
            
            # Check pitch_sequences count
            cur.execute("SELECT COUNT(*) FROM pitch_sequences")
            pitch_sequences_count = cur.fetchone()[0]
            
            # Check date range
            cur.execute("SELECT MIN(game_date), MAX(game_date) FROM atbats")
            date_range = cur.fetchone()
            
            # Check sample data
            cur.execute("SELECT * FROM atbats LIMIT 3")
            sample_atbats = cur.fetchall()
            
            print(f"📊 Migration Status:")
            print(f"   At-bats: {atbats_count:,}")
            print(f"   Pitch sequences: {pitch_sequences_count:,}")
            print(f"   Date range: {date_range[0]} to {date_range[1]}")
            print(f"   Sample at-bats: {len(sample_atbats)}")
            
            if sample_atbats:
                print(f"   First at-bat: Game {sample_atbats[0][1]}, At-bat {sample_atbats[0][2]}")
            
        conn.close()
        
    except Exception as e:
        print(f"❌ Error checking migration status: {e}")

if __name__ == "__main__":
    check_migration_status() 