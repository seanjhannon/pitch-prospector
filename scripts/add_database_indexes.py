#!/usr/bin/env python3
"""
Add database indexes to improve query performance.
Focuses on columns used in the main app queries.
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    """Get database connection using environment variables."""
    return psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=os.environ.get("SUPABASE_DB_PORT", 5432),
        dbname=os.environ["SUPABASE_DB_NAME"],
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"]
    )

def add_indexes():
    """Add performance indexes to the database."""
    indexes = [
        # Primary query indexes
        "CREATE INDEX IF NOT EXISTS idx_atbats_game_date ON atbats_simple(game_date)",
        "CREATE INDEX IF NOT EXISTS idx_atbats_pitch_sequence_hash ON atbats_simple(pitch_sequence_hash)",
        "CREATE INDEX IF NOT EXISTS idx_atbats_date_hash ON atbats_simple(game_date, pitch_sequence_hash)",
        
        # Additional useful indexes
        "CREATE INDEX IF NOT EXISTS idx_atbats_pitcher ON atbats_simple(pitcher)",
        "CREATE INDEX IF NOT EXISTS idx_atbats_batter ON atbats_simple(batter)",
        "CREATE INDEX IF NOT EXISTS idx_atbats_game_pk ON atbats_simple(game_pk)",
        
        # Composite indexes for common query patterns
        "CREATE INDEX IF NOT EXISTS idx_atbats_date_pitcher ON atbats_simple(game_date, pitcher)",
        "CREATE INDEX IF NOT EXISTS idx_atbats_date_batter ON atbats_simple(game_date, batter)",
    ]
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            print("🔧 Adding database indexes...")
            for i, index_sql in enumerate(indexes, 1):
                try:
                    print(f"  {i}/{len(indexes)}: Adding index...")
                    cur.execute(index_sql)
                    print(f"  ✅ Index created successfully")
                except Exception as e:
                    print(f"  ⚠️  Index creation failed: {e}")
            
            conn.commit()
            print("🎉 Database indexing completed!")

def check_existing_indexes():
    """Check what indexes already exist."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT indexname, indexdef 
                FROM pg_indexes 
                WHERE tablename = 'atbats_simple'
                ORDER BY indexname
            """)
            indexes = cur.fetchall()
            
            print("📊 Existing indexes on atbats_simple table:")
            for name, definition in indexes:
                print(f"  - {name}")
                print(f"    {definition[:100]}...")

if __name__ == "__main__":
    print("🔍 Checking existing indexes...")
    check_existing_indexes()
    print()
    
    add_indexes()
    
    print("\n🔍 Checking indexes after creation...")
    check_existing_indexes() 