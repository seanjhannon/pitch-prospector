#!/usr/bin/env python3
"""
Migrate data in batches to avoid connection timeouts.
"""

import os
import psycopg2
from dotenv import load_dotenv
import time

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

def migrate_in_batches():
    """Migrate data in small batches to avoid timeouts."""
    print("🔧 Starting batch migration...")
    start_time = time.time()
    
    batch_size = 10000  # Smaller batches
    total_migrated = 0
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get total count
            cur.execute("SELECT COUNT(*) FROM atbats_simple")
            total_records = cur.fetchone()[0]
            print(f"📊 Total records to migrate: {total_records:,}")
            
            # Migrate in batches
            for offset in range(0, total_records, batch_size):
                try:
                    print(f"📊 Migrating batch {offset//batch_size + 1} (offset {offset:,})...")
                    
                    cur.execute("""
                        INSERT INTO atbats_simple_optimized (
                            game_pk, at_bat_number, game_date, batter, pitcher, inning,
                            pitch_sequence, pitch_level_data
                        )
                        SELECT 
                            game_pk, at_bat_number, game_date, batter, pitcher, inning,
                            pitch_sequence, pitch_level_data
                        FROM atbats_simple
                        ORDER BY id
                        LIMIT %s OFFSET %s
                        ON CONFLICT (game_pk, at_bat_number) DO NOTHING
                    """, (batch_size, offset))
                    
                    batch_count = cur.rowcount
                    total_migrated += batch_count
                    
                    print(f"  ✅ Migrated {batch_count:,} records (total: {total_migrated:,})")
                    
                    # Commit each batch
                    conn.commit()
                    
                    # Small delay to avoid overwhelming the database
                    time.sleep(0.1)
                    
                except Exception as e:
                    print(f"  ❌ Error in batch {offset//batch_size + 1}: {e}")
                    conn.rollback()
                    # Continue with next batch
                    continue
    
    duration = time.time() - start_time
    print(f"✅ Batch migration completed in {duration:.2f}s")
    print(f"📊 Total records migrated: {total_migrated:,}")

def verify_migration():
    """Verify that migration was successful."""
    print("\n🔍 Verifying migration...")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Count records in both tables
            cur.execute("SELECT COUNT(*) FROM atbats_simple")
            original_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM atbats_simple_optimized")
            optimized_count = cur.fetchone()[0]
            
            print(f"📊 Record counts:")
            print(f"  • Original table: {original_count:,}")
            print(f"  • Optimized table: {optimized_count:,}")
            
            if original_count == optimized_count:
                print("  ✅ Migration successful - all records migrated")
            else:
                print(f"  ⚠️  Migration incomplete - {original_count - optimized_count:,} records missing")

if __name__ == "__main__":
    print("🚀 Starting batch migration...")
    print("=" * 50)
    
    migrate_in_batches()
    verify_migration()
    
    print("\n" + "=" * 50)
    print("🎉 Batch migration completed!") 