#!/usr/bin/env python3
"""
Simple optimization: add GIN index and drop hash column.
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    """Get database connection."""
    return psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=os.environ.get("SUPABASE_DB_PORT", 5432),
        dbname=os.environ["SUPABASE_DB_NAME"],
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"]
    )

def main():
    """Simple optimization process."""
    print("🚀 Starting simple table optimization...")
    
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                print("🔧 Adding GIN index on pitch_sequence...")
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_pitch_sequence_gin 
                    ON atbats_simple USING GIN(pitch_sequence)
                """)
                print("✅ GIN index created")
                
                print("🔧 Dropping pitch_sequence_hash column...")
                cur.execute("ALTER TABLE atbats_simple DROP COLUMN IF EXISTS pitch_sequence_hash")
                print("✅ Hash column dropped")
                
                print("🔧 Dropping hash-related indexes...")
                cur.execute("DROP INDEX IF EXISTS idx_atbats_pitch_sequence_hash")
                cur.execute("DROP INDEX IF EXISTS idx_atbats_date_hash")
                print("✅ Hash indexes dropped")
                
                conn.commit()
                print("🎉 Optimization completed!")
                
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main() 