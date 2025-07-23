#!/usr/bin/env python3
"""
Clean up existing data before running single-table migration
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

def cleanup_existing_data():
    try:
        conn = get_supabase_connection()
        print("✅ Connected to Supabase")
        
        with conn.cursor() as cur:
            # Check current data
            cur.execute("SELECT COUNT(*) FROM atbats")
            atbats_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM pitch_sequences")
            pitch_sequences_count = cur.fetchone()[0]
            
            print(f"📊 Current data:")
            print(f"   At-bats: {atbats_count:,}")
            print(f"   Pitch sequences: {pitch_sequences_count:,}")
            
            if atbats_count > 0 or pitch_sequences_count > 0:
                print("🗑️  Cleaning up existing data...")
                
                # Delete in correct order (pitch_sequences first due to foreign key)
                cur.execute("DELETE FROM pitch_sequences")
                print(f"   Deleted {pitch_sequences_count:,} pitch sequences")
                
                cur.execute("DELETE FROM atbats")
                print(f"   Deleted {atbats_count:,} at-bats")
                
                conn.commit()
                print("✅ Cleanup completed!")
            else:
                print("✅ No existing data to clean up")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")

if __name__ == "__main__":
    cleanup_existing_data() 