#!/usr/bin/env python3
"""
Command-line based migration script for large Pitch Prospector datasets.
Uses pg_dump and psql for maximum speed and efficiency.
"""

import sys
import os
import subprocess
import logging
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

# Add the parent directory to the path to import the config
sys.path.append(str(Path(__file__).parent.parent))

try:
    from cockroach_streamlit_connection import get_cockroach_connection
except ImportError:
    print("❌ Could not import CockroachDB connection module")
    print("Please ensure cockroach_streamlit_connection.py is in the project root")
    sys.exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('command_line_migration_log.txt'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def check_dependencies():
    """Check if required command-line tools are available."""
    logger.info("🔍 Checking required dependencies...")
    
    required_tools = ['pg_dump', 'psql']
    missing_tools = []
    
    for tool in required_tools:
        try:
            result = subprocess.run([tool, '--version'], 
                                  capture_output=True, text=True, check=True)
            logger.info(f"✅ {tool}: {result.stdout.strip()}")
        except (subprocess.CalledProcessError, FileNotFoundError):
            missing_tools.append(tool)
            logger.warning(f"⚠️ {tool} not found")
    
    if missing_tools:
        logger.error(f"❌ Missing required tools: {', '.join(missing_tools)}")
        logger.error("Please install PostgreSQL client tools:")
        logger.error("  macOS: brew install postgresql")
        logger.error("  Ubuntu: sudo apt-get install postgresql-client")
        logger.error("  Windows: Download from https://www.postgresql.org/download/windows/")
        return False
    
    return True

def get_supabase_connection_string():
    """Get Supabase connection string from environment or secrets."""
    # Try environment variables first
    host = os.getenv('SUPABASE_DB_HOST')
    db = os.getenv('SUPABASE_DB_NAME')
    user = os.getenv('SUPABASE_DB_USER')
    password = os.getenv('SUPABASE_DB_PASSWORD')
    port = os.getenv('SUPABASE_DB_PORT', '5432')
    
    if all([host, db, user, password]):
        return f"postgresql://{user}:{password}@{host}:{port}/{db}"
    
    # Fallback to hardcoded values (you can update these)
    logger.warning("⚠️ Using hardcoded Supabase connection (not recommended)")
    return "postgresql://postgres:[YOUR-PASSWORD]@db.xjjwtmcoklsqosxkexqw.supabase.co:5432/postgres"

def get_cockroach_connection_string():
    """Get CockroachDB connection string."""
    try:
        from cockroach_config import get_cockroach_connection_string
        return get_cockroach_connection_string()
    except ImportError:
        # Fallback to hardcoded values
        logger.warning("⚠️ Using hardcoded CockroachDB connection")
        return "postgresql://sean:_GS_iQHq4ZjjvwA4-VBqcQ@pitches-8229.jxf.gcp-us-west2.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full&sslrootcert=~/.postgresql/root.crt"

def dump_supabase_data(output_file: str = "supabase_dump.sql"):
    """Dump data from Supabase using pg_dump."""
    logger.info(f"📥 Dumping data from Supabase to {output_file}...")
    
    supabase_conn = get_supabase_connection_string()
    
    # pg_dump command for data only (no schema)
    cmd = [
        'pg_dump',
        '--data-only',           # Only data, no schema
        '--table=atbats_optimized',  # Specific table
        '--no-owner',            # No ownership info
        '--no-privileges',       # No privilege info
        '--verbose',             # Show progress
        '--file=' + output_file, # Output file
        supabase_conn            # Connection string
    ]
    
    logger.info(f"🔧 Running: {' '.join(cmd[:6])}... [connection string hidden]")
    
    try:
        start_time = time.time()
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        dump_time = time.time() - start_time
        logger.info(f"✅ Dump completed in {dump_time:.2f}s")
        
        # Check file size
        if os.path.exists(output_file):
            size_mb = os.path.getsize(output_file) / (1024 * 1024)
            logger.info(f"📁 Dump file size: {size_mb:.2f} MB")
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ pg_dump failed: {e}")
        logger.error(f"stderr: {e.stderr}")
        return False

def restore_to_cockroachdb(dump_file: str):
    """Restore data to CockroachDB using psql."""
    logger.info(f"📤 Restoring data to CockroachDB from {dump_file}...")
    
    cockroach_conn = get_cockroach_connection_string()
    
    # psql command to restore data
    cmd = [
        'psql',
        '--echo-all',           # Echo all commands
        '--verbose',            # Show progress
        '--file=' + dump_file,  # Input file
        cockroach_conn          # Connection string
    ]
    
    logger.info(f"🔧 Running: {' '.join(cmd[:4])}... [connection string hidden]")
    
    try:
        start_time = time.time()
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        restore_time = time.time() - start_time
        logger.info(f"✅ Restore completed in {restore_time:.2f}s")
        
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ psql restore failed: {e}")
        logger.error(f"stderr: {e.stderr}")
        return False

def verify_migration():
    """Verify that the migration was successful."""
    logger.info("🔍 Verifying migration...")
    
    try:
        conn = get_cockroach_connection()
        
        with conn.pool.connection() as conn_obj:
            with conn_obj.cursor() as cursor:
                
                # Get record count
                cursor.execute("SELECT COUNT(*) FROM atbats_optimized;")
                count = cursor.fetchone()[0]
                logger.info(f"📊 Total records in CockroachDB: {count:,}")
                
                # Check for sample data
                cursor.execute("""
                    SELECT id, game_pk, game_date, batter, pitcher 
                    FROM atbats_optimized 
                    ORDER BY id 
                    LIMIT 5;
                """)
                
                sample_records = cursor.fetchall()
                if sample_records:
                    logger.info("✅ Sample records found:")
                    for record in sample_records:
                        logger.info(f"  ID: {record[0]}, Game: {record[1]}, Date: {record[2]}, Batter: {record[3]}, Pitcher: {record[4]}")
                else:
                    logger.warning("⚠️ No sample records found")
                
                conn.close()
                return count > 0
                
    except Exception as e:
        logger.error(f"❌ Migration verification failed: {e}")
        return False

def cleanup_dump_file(dump_file: str):
    """Clean up the temporary dump file."""
    try:
        if os.path.exists(dump_file):
            os.remove(dump_file)
            logger.info(f"🧹 Cleaned up dump file: {dump_file}")
    except Exception as e:
        logger.warning(f"⚠️ Failed to clean up dump file: {e}")

def main():
    """Main migration function."""
    logger.info("🚀 Starting command-line migration to CockroachDB...")
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Configuration
    dump_file = "supabase_dump.sql"
    cleanup_after = True  # Set to False if you want to keep the dump file
    
    try:
        # Step 1: Dump from Supabase
        if not dump_supabase_data(dump_file):
            logger.error("❌ Failed to dump data from Supabase")
            sys.exit(1)
        
        # Step 2: Restore to CockroachDB
        if not restore_to_cockroachdb(dump_file):
            logger.error("❌ Failed to restore data to CockroachDB")
            sys.exit(1)
        
        # Step 3: Verify migration
        if not verify_migration():
            logger.error("❌ Migration verification failed")
            sys.exit(1)
        
        logger.info("🎉 Command-line migration completed successfully!")
        logger.info("📋 Next steps:")
        logger.info("  1. Test your application with CockroachDB")
        logger.info("  2. Monitor performance and query times")
        logger.info("  3. Consider removing Supabase connection when ready")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        return False
        
    finally:
        # Clean up
        if cleanup_after:
            cleanup_dump_file(dump_file)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 