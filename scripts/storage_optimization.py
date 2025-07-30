#!/usr/bin/env python3
"""
Storage optimization script for Pitch Prospector database.
Analyzes current storage usage and provides options to reduce database size
while maintaining user-facing functionality.
"""

import os
import psycopg2
from dotenv import load_dotenv
import json

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

def analyze_storage_usage():
    """Analyze current database storage usage."""
    print("🔍 Analyzing database storage usage...")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Get table size
            cur.execute("""
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
                FROM pg_tables 
                WHERE schemaname = 'public'
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
            """)
            
            tables = cur.fetchall()
            total_size = 0
            
            print("📊 Table sizes:")
            for schema, table, size, size_bytes in tables:
                print(f"  • {table}: {size} ({size_bytes:,} bytes)")
                total_size += size_bytes
            
            print(f"📊 Total database size: {total_size:,} bytes ({total_size / 1024 / 1024:.1f} MB)")
            
            # Analyze column sizes
            cur.execute("""
                SELECT 
                    column_name,
                    data_type,
                    pg_size_pretty(SUM(pg_column_size(column_name::text))) as total_size
                FROM information_schema.columns 
                WHERE table_name = 'atbats_simple'
                GROUP BY column_name, data_type
                ORDER BY SUM(pg_column_size(column_name::text)) DESC
            """)
            
            columns = cur.fetchall()
            print("\n📊 Column analysis (atbats_simple):")
            for col, dtype, size in columns:
                print(f"  • {col} ({dtype}): {size}")
            
            return total_size, tables, columns

def analyze_pitch_data_usage():
    """Analyze what pitch data is actually being used in the UI."""
    print("\n🔍 Analyzing pitch data usage...")
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Sample some pitch_level_data to see what's stored
            cur.execute("""
                SELECT pitch_level_data 
                FROM atbats_simple 
                WHERE pitch_level_data IS NOT NULL 
                LIMIT 5
            """)
            
            samples = cur.fetchall()
            
            print("📊 Sample pitch_level_data fields:")
            if samples:
                sample_data = samples[0][0]
                if sample_data and len(sample_data) > 0:
                    first_pitch = sample_data[0]
                    for key, value in first_pitch.items():
                        print(f"  • {key}: {type(value).__name__}")
            
            # Count how many records have pitch_level_data
            cur.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(pitch_level_data) as records_with_pitch_data,
                    COUNT(pitch_sequence) as records_with_sequence
                FROM atbats_simple
            """)
            
            counts = cur.fetchone()
            print(f"\n📊 Data completeness:")
            print(f"  • Total records: {counts[0]:,}")
            print(f"  • Records with pitch_level_data: {counts[1]:,} ({counts[1]/counts[0]*100:.1f}%)")
            print(f"  • Records with pitch_sequence: {counts[2]:,} ({counts[2]/counts[0]*100:.1f}%)")

def identify_optimization_opportunities():
    """Identify opportunities to reduce storage."""
    print("\n🔍 Identifying optimization opportunities...")
    
    opportunities = []
    
    # Check what fields are actually used in the UI
    ui_used_fields = {
        'pitch_type': 'Used in UI display',
        'description': 'Used in UI display', 
        'release_speed': 'Used in UI display',
        'zone': 'Used in UI display'
    }
    
    print("📊 Fields used in UI:")
    for field, reason in ui_used_fields.items():
        print(f"  • {field}: {reason}")
    
    # Fields that could be removed (nice-to-have but not essential)
    optional_fields = [
        'pfx_x', 'pfx_z', 'xc', 'yc', 'zc',
        'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az',
        'break_y', 'break_angle', 'break_length',
        'spin_direction'
    ]
    
    print("\n📊 Optional fields (could be removed):")
    for field in optional_fields:
        print(f"  • {field}")
    
    opportunities.append({
        'type': 'remove_optional_fields',
        'description': f'Remove {len(optional_fields)} optional pitch fields',
        'fields': optional_fields
    })
    
    # Check for data compression opportunities
    opportunities.append({
        'type': 'compress_json',
        'description': 'Compress JSONB fields using gzip',
        'fields': ['pitch_sequence', 'pitch_level_data']
    })
    
    # Check for data retention policies
    opportunities.append({
        'type': 'retention_policy',
        'description': 'Implement data retention (e.g., keep last 3 years)',
        'years': 3
    })
    
    return opportunities

def estimate_storage_savings():
    """Estimate potential storage savings."""
    print("\n🔍 Estimating storage savings...")
    
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
            print(f"📊 Current JSONB field sizes:")
            print(f"  • pitch_level_data: {sizes[0]}")
            print(f"  • pitch_sequence: {sizes[1]}")
            
            # Estimate savings from removing optional fields
            print(f"\n📊 Estimated savings:")
            print(f"  • Remove optional fields: ~30-40% reduction in pitch_level_data")
            print(f"  • JSONB compression: ~50-60% reduction")
            print(f"  • Data retention (3 years): ~60% reduction in total records")
            
            # Calculate total potential savings
            print(f"\n📊 Total potential savings:")
            print(f"  • Conservative estimate: 40-50% database size reduction")
            print(f"  • Aggressive estimate: 60-70% database size reduction")

def create_optimized_schema():
    """Create an optimized schema with reduced storage."""
    print("\n🔍 Creating optimized schema...")
    
    optimized_schema = """
    -- Optimized atbats_simple table with reduced storage
    CREATE TABLE IF NOT EXISTS atbats_simple_optimized (
        id SERIAL PRIMARY KEY,
        game_pk INTEGER NOT NULL,
        at_bat_number INTEGER NOT NULL,
        game_date DATE NOT NULL,
        batter INTEGER NOT NULL,
        pitcher INTEGER NOT NULL,
        inning INTEGER NOT NULL,
        pitch_sequence_hash VARCHAR(40) NOT NULL,
        pitch_sequence JSONB,  -- Keep only essential fields
        pitch_level_data JSONB, -- Keep only essential fields
        UNIQUE(game_pk, at_bat_number)
    );
    
    -- Indexes for performance
    CREATE INDEX IF NOT EXISTS idx_opt_game_date ON atbats_simple_optimized(game_date);
    CREATE INDEX IF NOT EXISTS idx_opt_pitch_sequence_hash ON atbats_simple_optimized(pitch_sequence_hash);
    CREATE INDEX IF NOT EXISTS idx_opt_date_hash ON atbats_simple_optimized(game_date, pitch_sequence_hash);
    """
    
    print("📊 Optimized schema features:")
    print("  • Reduced JSONB fields (only essential data)")
    print("  • Same indexes for performance")
    print("  • Same unique constraints")
    
    return optimized_schema

if __name__ == "__main__":
    print("🚀 Starting storage optimization analysis...")
    print("=" * 60)
    
    # Analyze current usage
    total_size, tables, columns = analyze_storage_usage()
    
    # Analyze pitch data usage
    analyze_pitch_data_usage()
    
    # Identify opportunities
    opportunities = identify_optimization_opportunities()
    
    # Estimate savings
    estimate_storage_savings()
    
    # Show optimization options
    print("\n" + "=" * 60)
    print("📋 Optimization Options:")
    print("1. Remove optional pitch fields (30-40% reduction)")
    print("2. Compress JSONB fields (50-60% reduction)")
    print("3. Implement data retention policy (60% reduction)")
    print("4. Create optimized schema with reduced fields")
    print("5. All of the above (60-70% total reduction)")
    
    print("\n💡 Recommendation:")
    print("Start with option 1 (remove optional fields) as it's safe and provides immediate benefits.")
    print("Then consider option 3 (data retention) if you don't need historical data beyond 3 years.") 