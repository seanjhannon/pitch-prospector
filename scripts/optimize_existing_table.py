#!/usr/bin/env python3
"""
Optimize existing table by dropping pitch_sequence_hash column
and adding GIN index on pitch_sequence JSONB column.
Much faster than migrating data.
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

def analyze_before():
    """Analyze table before optimization."""
    print("🔍 Analyzing table before optimization...")
    
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
            
            return size_bytes

def add_gin_index():
    """Add GIN index on pitch_sequence JSONB column."""
    print("\n🔧 Adding GIN index on pitch_sequence column...")
    start_time = time.time()
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_pitch_sequence_gin 
                ON atbats_simple USING GIN(pitch_sequence)
            """)
            conn.commit()
    
    duration = time.time() - start_time
    print(f"✅ GIN index created in {duration:.2f}s")

def drop_hash_column():
    """Drop the pitch_sequence_hash column."""
    print("\n🔧 Dropping pitch_sequence_hash column...")
    start_time = time.time()
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Drop the column
            cur.execute("ALTER TABLE atbats_simple DROP COLUMN IF EXISTS pitch_sequence_hash")
            
            # Drop related indexes
            cur.execute("DROP INDEX IF EXISTS idx_atbats_pitch_sequence_hash")
            cur.execute("DROP INDEX IF EXISTS idx_atbats_date_hash")
            
            conn.commit()
    
    duration = time.time() - start_time
    print(f"✅ Hash column and indexes dropped in {duration:.2f}s")

def test_jsonb_queries():
    """Test query performance using JSONB directly."""
    print("\n🔍 Testing JSONB query performance...")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Test 1: Date range query
            print("📊 Testing date range query...")
            start_time = time.time()
            cur.execute("""
                SELECT COUNT(*) FROM atbats_simple 
                WHERE game_date BETWEEN '2025-01-01' AND '2025-07-25'
            """)
            count = cur.fetchone()[0]
            duration = time.time() - start_time
            print(f"  • Found {count:,} records in {duration:.3f}s")
            
            # Test 2: JSONB sequence query (replaces hash query)
            print("📊 Testing JSONB sequence query...")
            start_time = time.time()
            cur.execute("""
                SELECT COUNT(*) FROM atbats_simple 
                WHERE pitch_sequence = '[["FF", "called_strike"], ["SL", "ball"], ["FF", "swinging_strike"]]'::jsonb
            """)
            count = cur.fetchone()[0]
            duration = time.time() - start_time
            print(f"  • Found {count:,} records in {duration:.3f}s")
            
            # Test 3: Combined date and sequence query
            print("📊 Testing combined date and sequence query...")
            start_time = time.time()
            cur.execute("""
                SELECT COUNT(*) FROM atbats_simple 
                WHERE game_date BETWEEN '2025-01-01' AND '2025-07-25'
                AND pitch_sequence = '[["FF", "called_strike"], ["SL", "ball"], ["FF", "swinging_strike"]]'::jsonb
            """)
            count = cur.fetchone()[0]
            duration = time.time() - start_time
            print(f"  • Found {count:,} records in {duration:.3f}s")

def analyze_after():
    """Analyze table after optimization."""
    print("\n🔍 Analyzing table after optimization...")
    
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
            
            print(f"📊 Optimized table size: {table_size}")
            
            # Get remaining column sizes
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
            print("\n📊 Remaining column sizes:")
            for col, dtype, size in columns:
                print(f"  • {col} ({dtype}): {size}")
            
            return size_bytes

def main():
    """Main optimization process."""
    print("🚀 Starting table optimization...")
    print("=" * 60)
    
    # Step 1: Analyze before
    before_size = analyze_before()
    print()
    
    # Step 2: Add GIN index first (for performance)
    add_gin_index()
    print()
    
    # Step 3: Drop hash column
    drop_hash_column()
    print()
    
    # Step 4: Test performance
    test_jsonb_queries()
    print()
    
    # Step 5: Analyze after
    after_size = analyze_after()
    print()
    
    # Step 6: Calculate savings
    if before_size > 0 and after_size > 0:
        savings = before_size - after_size
        savings_pct = (savings / before_size) * 100
        print(f"📊 Storage savings: {savings:,} bytes ({savings_pct:.1f}%)")
    
    print("\n" + "=" * 60)
    print("🎉 Table optimization completed!")
    print("📋 Summary:")
    print("  • Added GIN index on pitch_sequence JSONB column")
    print("  • Dropped pitch_sequence_hash column and related indexes")
    print("  • JSONB queries now work directly without hash generation")
    print("\n💡 Next steps:")
    print("  • Update application code to remove hash generation")
    print("  • Test the application with new JSONB queries")

if __name__ == "__main__":
    main() 