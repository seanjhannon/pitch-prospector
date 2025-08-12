#!/usr/bin/env python3
"""
Setup script for CockroachDB migration of Pitch Prospector.
Tests connection and creates the optimized table schema.
"""

import sys
import os
import logging
from datetime import datetime
from pathlib import Path

# Add the parent directory to the path to import the config
sys.path.append(str(Path(__file__).parent.parent))

try:
    from cockroach_streamlit_cloud import get_cockroach_cloud_connection
except ImportError:
    print("❌ Could not import CockroachDB connection module")
    print("Please ensure cockroach_streamlit_cloud.py is in the project root")
    sys.exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_cockroach_connection():
    """Test the CockroachDB connection."""
    logger.info("🔍 Testing CockroachDB connection...")
    
    try:
        # Get connection
        conn = get_cockroach_cloud_connection()
        
        # Test basic query
        result = conn.pool.connection()
        with result as conn_obj:
            with conn_obj.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()
                logger.info(f"✅ Successfully connected to CockroachDB!")
                logger.info(f"📊 Version: {version[0]}")
                
                # Test database info
                cursor.execute("SELECT current_database(), current_user, inet_server_addr();")
                db_info = cursor.fetchone()
                logger.info(f"📁 Database: {db_info[0]}")
                logger.info(f"👤 User: {db_info[1]}")
                logger.info(f"🌐 Server: {db_info[2]}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        return False

def create_optimized_table():
    """Create the optimized table schema in CockroachDB."""
    logger.info("🔧 Creating optimized table schema...")
    
    try:
        conn = get_cockroach_cloud_connection()
        
        with conn.pool.connection() as conn_obj:
            with conn_obj.cursor() as cursor:
                
                # Check if table already exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = 'atbats_optimized'
                    );
                """)
                
                table_exists = cursor.fetchone()[0]
                
                if table_exists:
                    logger.info("ℹ️ Table 'atbats_optimized' already exists")
                    
                    # Show current schema
                    cursor.execute("""
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns 
                        WHERE table_name = 'atbats_optimized'
                        ORDER BY ordinal_position;
                    """)
                    
                    columns = cursor.fetchall()
                    logger.info("📋 Current table schema:")
                    for col in columns:
                        logger.info(f"  {col[0]}: {col[1]} (nullable: {col[2]}, default: {col[3]})")
                    
                else:
                    # Create the optimized table
                    logger.info("🏗️ Creating new table 'atbats_optimized'...")
                    
                    create_table_sql = """
                    CREATE TABLE atbats_optimized (
                        id BIGSERIAL PRIMARY KEY,
                        game_pk BIGINT NOT NULL,
                        at_bat_number INTEGER NOT NULL,
                        game_date DATE NOT NULL,
                        batter BIGINT NOT NULL,
                        pitcher BIGINT NOT NULL,
                        inning INTEGER NOT NULL,
                        pitch_sequence JSONB NOT NULL,
                        pitch_data JSONB NOT NULL,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    );
                    """
                    
                    cursor.execute(create_table_sql)
                    
                    # Create indexes for performance
                    logger.info("📊 Creating performance indexes...")
                    
                    indexes = [
                        "CREATE INDEX idx_opt_game_date ON atbats_optimized(game_date);",
                        "CREATE INDEX idx_opt_pitch_sequence ON atbats_optimized USING GIN(pitch_sequence);",
                        "CREATE INDEX idx_opt_date_sequence ON atbats_optimized(game_date, pitch_sequence);",
                        "CREATE INDEX idx_opt_pitcher ON atbats_optimized(pitcher);",
                        "CREATE INDEX idx_opt_batter ON atbats_optimized(batter);",
                        "CREATE INDEX idx_opt_game_atbat ON atbats_optimized(game_pk, at_bat_number);",
                        "CREATE INDEX idx_opt_created_at ON atbats_optimized(created_at);"
                    ]
                    
                    for index_sql in indexes:
                        try:
                            cursor.execute(index_sql)
                            logger.info(f"  ✅ Created index: {index_sql.split('ON')[1].strip()}")
                        except Exception as e:
                            logger.warning(f"  ⚠️ Index creation warning: {e}")
                    
                    # Create unique constraint
                    cursor.execute("""
                        ALTER TABLE atbats_optimized 
                        ADD CONSTRAINT unique_game_atbat 
                        UNIQUE(game_pk, at_bat_number);
                    """)
                    
                    logger.info("✅ Table 'atbats_optimized' created successfully!")
                    
                    # Show the new schema
                    cursor.execute("""
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns 
                        WHERE table_name = 'atbats_optimized'
                        ORDER BY ordinal_position;
                    """)
                    
                    columns = cursor.fetchall()
                    logger.info("📋 New table schema:")
                    for col in columns:
                        logger.info(f"  {col[0]}: {col[1]} (nullable: {col[2]}, default: {col[3]})")
                
                conn_obj.commit()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Table creation failed: {e}")
        return False

def test_table_operations():
    """Test basic table operations."""
    logger.info("🧪 Testing table operations...")
    
    try:
        conn = get_cockroach_cloud_connection()
        
        with conn.pool.connection() as conn_obj:
            with conn_obj.cursor() as cursor:
                
                # Test insert
                test_data = {
                    'game_pk': 123456789,
                    'at_bat_number': 1,
                    'game_date': '2024-01-01',
                    'batter': 12345,
                    'pitcher': 67890,
                    'inning': 1,
                    'pitch_sequence': [['FF', 'called_strike'], ['SL', 'ball']],
                    'pitch_data': [[95.2, 5], [87.1, 8]]
                }
                
                insert_sql = """
                INSERT INTO atbats_optimized 
                (game_pk, at_bat_number, game_date, batter, pitcher, inning, pitch_sequence, pitch_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_pk, at_bat_number) DO NOTHING;
                """
                
                cursor.execute(insert_sql, (
                    test_data['game_pk'], test_data['at_bat_number'], test_data['game_date'],
                    test_data['batter'], test_data['pitcher'], test_data['inning'],
                    test_data['pitch_sequence'], test_data['pitch_data']
                ))
                logger.info("✅ Test insert successful")
                
                # Test select
                cursor.execute("SELECT COUNT(*) FROM atbats_optimized;")
                count = cursor.fetchone()[0]
                logger.info(f"📊 Total records: {count}")
                
                # Test JSONB query
                cursor.execute("""
                    SELECT id, pitch_sequence 
                    FROM atbats_optimized 
                    WHERE pitch_sequence @> '[["FF", "called_strike"]]'::jsonb
                    LIMIT 1;
                """)
                
                result = cursor.fetchone()
                if result:
                    logger.info(f"✅ JSONB query successful: Found record {result[0]}")
                else:
                    logger.info("ℹ️ No records found for JSONB query (expected for test data)")
                
                # Clean up test data
                cursor.execute("DELETE FROM atbats_optimized WHERE game_pk = 123456789;")
                logger.info("🧹 Cleaned up test data")
                
                conn_obj.commit()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Table operations test failed: {e}")
        return False

def main():
    """Main setup function."""
    logger.info("🚀 Starting CockroachDB setup for Pitch Prospector...")
    
    # Test connection
    if not test_cockroach_connection():
        logger.error("❌ Connection test failed. Exiting.")
        return False
    
    # Create table
    if not create_optimized_table():
        logger.error("❌ Table creation failed. Exiting.")
        return False
    
    # Test operations
    if not test_table_operations():
        logger.error("❌ Table operations test failed.")
        return False
    
    logger.info("🎉 CockroachDB setup completed successfully!")
    logger.info("📋 Next steps:")
    logger.info("  1. Update your app.py to use CockroachDB connection")
    logger.info("  2. Test data migration from Supabase")
    logger.info("  3. Update connection strings in your application")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 