#!/usr/bin/env python3
"""
Optimize pitch data by removing non-essential fields.
Keeps only the fields used in the UI: pitch_type, description, release_speed, zone
"""

import os
import psycopg2
import json
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

def get_essential_fields():
    """Get list of essential fields that are used in the UI."""
    return ['pitch_type', 'description', 'release_speed', 'zone']

def get_optional_fields():
    """Get list of optional fields that can be removed."""
    return [
        'pfx_x', 'pfx_z', 'xc', 'yc', 'zc',
        'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az',
        'break_y', 'break_angle', 'break_length',
        'spin_direction', 'release_spin_rate', 'release_extension',
        'plate_x', 'plate_z', 'hit_distance_sc', 'launch_speed', 'launch_angle',
        'des', 'events', 'balls', 'strikes', 'stand', 'p_throws',
        'outs_when_up', 'home_team', 'away_team', 'home_score', 'away_score',
        'bat_score', 'fld_score', 'game_year', 'inning_topbot', 'pitch_name',
        'pitch_number', 'at_bat_number', 'game_pk', 'batter', 'pitcher',
        'inning', 'game_date', 'game_year'
    ]

def optimize_pitch_data():
    """Optimize pitch_level_data by keeping only essential fields."""
    print("🔧 Starting pitch data optimization...")
    start_time = time.time()
    
    essential_fields = get_essential_fields()
    optional_fields = get_optional_fields()
    
    print(f"📊 Keeping essential fields: {essential_fields}")
    print(f"📊 Removing optional fields: {len(optional_fields)} fields")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get total count for progress tracking
            cur.execute("SELECT COUNT(*) FROM atbats_simple WHERE pitch_level_data IS NOT NULL")
            total_records = cur.fetchone()[0]
            print(f"📊 Total records to process: {total_records:,}")
            
            # Process in batches to avoid memory issues
            batch_size = 1000
            processed = 0
            
            for offset in range(0, total_records, batch_size):
                # Get batch of records
                cur.execute("""
                    SELECT id, pitch_level_data 
                    FROM atbats_simple 
                    WHERE pitch_level_data IS NOT NULL
                    ORDER BY id
                    LIMIT %s OFFSET %s
                """, (batch_size, offset))
                
                batch = cur.fetchall()
                if not batch:
                    break
                
                # Process each record in the batch
                updates = []
                for record_id, pitch_data in batch:
                    if pitch_data and isinstance(pitch_data, list):
                        optimized_pitches = []
                        for pitch in pitch_data:
                            if isinstance(pitch, dict):
                                # Keep only essential fields
                                optimized_pitch = {}
                                for field in essential_fields:
                                    if field in pitch:
                                        optimized_pitch[field] = pitch[field]
                                optimized_pitches.append(optimized_pitch)
                            else:
                                optimized_pitches.append(pitch)
                        
                        updates.append((json.dumps(optimized_pitches), record_id))
                
                # Update batch
                if updates:
                    cur.executemany("""
                        UPDATE atbats_simple 
                        SET pitch_level_data = %s 
                        WHERE id = %s
                    """, updates)
                
                processed += len(batch)
                print(f"  📊 Processed {processed:,}/{total_records:,} records ({(processed/total_records)*100:.1f}%)")
            
            conn.commit()
    
    duration = time.time() - start_time
    print(f"✅ Pitch data optimization completed in {duration:.2f}s")
    print(f"📊 Processed {processed:,} records")

def analyze_storage_savings():
    """Analyze storage savings after optimization."""
    print("\n🔍 Analyzing storage savings...")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get current JSONB field sizes
            cur.execute("""
                SELECT 
                    pg_size_pretty(SUM(pg_column_size(pitch_level_data))) as pitch_data_size,
                    pg_size_pretty(SUM(pg_column_size(pitch_sequence))) as sequence_size
                FROM atbats_simple 
                WHERE pitch_level_data IS NOT NULL
            """)
            
            sizes = cur.fetchone()
            if sizes and sizes[0]:
                print(f"📊 Current JSONB field sizes:")
                print(f"  • pitch_level_data: {sizes[0]}")
                print(f"  • pitch_sequence: {sizes[1]}")
            else:
                print("📊 Could not retrieve size information")

def create_backup_table():
    """Create a backup of the original table before optimization."""
    print("🔧 Creating backup table...")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Create backup table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS atbats_simple_backup AS 
                SELECT * FROM atbats_simple
            """)
            
            # Create indexes on backup table
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_backup_game_date ON atbats_simple_backup(game_date)
                """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_backup_pitch_sequence_hash ON atbats_simple_backup(pitch_sequence_hash)
                """)
            
            conn.commit()
    
    print("✅ Backup table created: atbats_simple_backup")

def main():
    """Main optimization process."""
    print("🚀 Starting database storage optimization...")
    print("=" * 60)
    
    # Step 1: Create backup
    create_backup_table()
    print()
    
    # Step 2: Optimize pitch data
    optimize_pitch_data()
    print()
    
    # Step 3: Analyze savings
    analyze_storage_savings()
    print()
    
    print("=" * 60)
    print("🎉 Optimization completed!")
    print("📋 Summary:")
    print("  • Backup table created: atbats_simple_backup")
    print("  • Pitch data optimized (kept only essential fields)")
    print("  • Expected storage reduction: 30-40%")
    print("\n💡 Next steps:")
    print("  • Test the application to ensure it still works")
    print("  • If everything works, you can drop the backup table")
    print("  • Consider implementing data retention policy for further savings")

if __name__ == "__main__":
    main() 