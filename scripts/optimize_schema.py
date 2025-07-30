#!/usr/bin/env python3
"""
Optimize database schema by removing pitch_sequence_hash column
and indexing pitch_sequence JSONB column directly.
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

def analyze_current_schema():
    """Analyze current schema and storage usage."""
    print("🔍 Analyzing current schema...")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get table size
            cur.execute("""
                SELECT 
                    pg_size_pretty(pg_total_relation_size('atbats_simple')) as table_size,
                    pg_total_relation_size('atbats_simple') as size_bytes
            """)
            result = cur.fetchone()
            table_size = result[0] if result else "Unknown"
            size_bytes = result[1] if result else 0
            
            print(f"📊 Current table size: {table_size}")
            
            # Get column sizes
            cur.execute("""
                SELECT 
                    column_name,
                    data_type,
                    pg_size_pretty(SUM(pg_column_size(column_name::text))) as column_size
                FROM information_schema.columns 
                WHERE table_name = 'atbats_simple'
                GROUP BY column_name, data_type
                ORDER BY SUM(pg_column_size(column_name::text)) DESC
            """)
            
            columns = cur.fetchall()
            print("\n📊 Column sizes:")
            for col, dtype, size in columns:
                print(f"  • {col} ({dtype}): {size}")
            
            return size_bytes, columns

def create_optimized_table():
    """Create optimized table without pitch_sequence_hash column."""
    print("\n🔧 Creating optimized table...")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Create new optimized table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS atbats_simple_optimized (
                    id SERIAL PRIMARY KEY,
                    game_pk BIGINT NOT NULL,
                    at_bat_number INTEGER NOT NULL,
                    game_date DATE NOT NULL,
                    batter BIGINT NOT NULL,
                    pitcher BIGINT NOT NULL,
                    inning INTEGER NOT NULL,
                    pitch_sequence JSONB,
                    pitch_level_data JSONB,
                    UNIQUE(game_pk, at_bat_number)
                )
            """)
            
            # Create indexes for performance
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opt_game_date ON atbats_simple_optimized(game_date)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opt_pitch_sequence ON atbats_simple_optimized USING GIN(pitch_sequence)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opt_date_sequence ON atbats_simple_optimized(game_date, pitch_sequence)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opt_pitcher ON atbats_simple_optimized(pitcher)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_opt_batter ON atbats_simple_optimized(batter)")
            
            conn.commit()
    
    print("✅ Optimized table created: atbats_simple_optimized")

def migrate_data():
    """Migrate data from old table to optimized table."""
    print("\n🔧 Migrating data to optimized table...")
    start_time = time.time()
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Copy data without pitch_sequence_hash column
            cur.execute("""
                INSERT INTO atbats_simple_optimized (
                    game_pk, at_bat_number, game_date, batter, pitcher, inning,
                    pitch_sequence, pitch_level_data
                )
                SELECT 
                    game_pk, at_bat_number, game_date, batter, pitcher, inning,
                    pitch_sequence, pitch_level_data
                FROM atbats_simple
                ON CONFLICT (game_pk, at_bat_number) DO NOTHING
            """)
            
            conn.commit()
    
    duration = time.time() - start_time
    print(f"✅ Data migration completed in {duration:.2f}s")

def test_optimized_queries():
    """Test query performance on optimized table."""
    print("\n🔍 Testing optimized query performance...")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Test 1: Date range query
            print("📊 Testing date range query...")
            start_time = time.time()
            cur.execute("""
                SELECT COUNT(*) FROM atbats_simple_optimized 
                WHERE game_date BETWEEN '2025-01-01' AND '2025-07-25'
            """)
            count = cur.fetchone()[0]
            duration = time.time() - start_time
            print(f"  • Found {count:,} records in {duration:.3f}s")
            
            # Test 2: JSONB sequence query (equivalent to old hash query)
            print("📊 Testing JSONB sequence query...")
            start_time = time.time()
            cur.execute("""
                SELECT COUNT(*) FROM atbats_simple_optimized 
                WHERE pitch_sequence = '[["FF", "called_strike"], ["SL", "ball"], ["FF", "swinging_strike"]]'::jsonb
            """)
            count = cur.fetchone()[0]
            duration = time.time() - start_time
            print(f"  • Found {count:,} records in {duration:.3f}s")
            
            # Test 3: Combined date and sequence query
            print("📊 Testing combined date and sequence query...")
            start_time = time.time()
            cur.execute("""
                SELECT COUNT(*) FROM atbats_simple_optimized 
                WHERE game_date BETWEEN '2025-01-01' AND '2025-07-25'
                AND pitch_sequence = '[["FF", "called_strike"], ["SL", "ball"], ["FF", "swinging_strike"]]'::jsonb
            """)
            count = cur.fetchone()[0]
            duration = time.time() - start_time
            print(f"  • Found {count:,} records in {duration:.3f}s")

def compare_storage():
    """Compare storage usage between old and optimized tables."""
    print("\n🔍 Comparing storage usage...")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get old table size
            cur.execute("""
                SELECT pg_size_pretty(pg_total_relation_size('atbats_simple')) as old_size,
                       pg_total_relation_size('atbats_simple') as old_bytes
            """)
            old_result = cur.fetchone()
            old_size = old_result[0] if old_result else "Unknown"
            old_bytes = old_result[1] if old_result else 0
            
            # Get optimized table size
            cur.execute("""
                SELECT pg_size_pretty(pg_total_relation_size('atbats_simple_optimized')) as opt_size,
                       pg_total_relation_size('atbats_simple_optimized') as opt_bytes
            """)
            opt_result = cur.fetchone()
            opt_size = opt_result[0] if opt_result else "Unknown"
            opt_bytes = opt_result[1] if opt_result else 0
            
            print(f"📊 Storage comparison:")
            print(f"  • Original table: {old_size}")
            print(f"  • Optimized table: {opt_size}")
            
            if old_bytes > 0 and opt_bytes > 0:
                savings = old_bytes - opt_bytes
                savings_pct = (savings / old_bytes) * 100
                print(f"  • Storage savings: {savings:,} bytes ({savings_pct:.1f}%)")

def main():
    """Main optimization process."""
    print("🚀 Starting schema optimization...")
    print("=" * 60)
    
    # Step 1: Analyze current schema
    size_bytes, columns = analyze_current_schema()
    print()
    
    # Step 2: Create optimized table
    create_optimized_table()
    print()
    
    # Step 3: Migrate data
    migrate_data()
    print()
    
    # Step 4: Test performance
    test_optimized_queries()
    print()
    
    # Step 5: Compare storage
    compare_storage()
    print()
    
    print("=" * 60)
    print("🎉 Schema optimization completed!")
    print("📋 Summary:")
    print("  • Removed pitch_sequence_hash column")
    print("  • Added GIN index on pitch_sequence JSONB column")
    print("  • Created optimized table: atbats_simple_optimized")
    print("\n💡 Next steps:")
    print("  • Test the application with optimized table")
    print("  • If performance is good, rename tables")
    print("  • Update application code to remove hash generation")

if __name__ == "__main__":
    main() 