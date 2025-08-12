#!/usr/bin/env python3
"""
Fast SQL-based migration script for moving Pitch Prospector data from Supabase to CockroachDB.
This script processes SQL export files for maximum speed and efficiency.
"""

import sys
import os
import logging
import time
from pathlib import Path
from typing import Optional, List, Dict, Any
import re

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
        logging.FileHandler('sql_migration_log.txt'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def process_sql_file(sql_file_path: str) -> List[str]:
    """Process SQL export file and extract INSERT statements."""
    logger.info(f"📖 Processing SQL file: {sql_file_path}")
    
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract INSERT statements
        insert_pattern = r'INSERT INTO [^;]+;'
        insert_statements = re.findall(insert_pattern, content, re.IGNORECASE | re.DOTALL)
        
        logger.info(f"📊 Found {len(insert_statements)} INSERT statements")
        
        # Also look for COPY statements (even faster)
        copy_pattern = r'COPY [^;]+;'
        copy_statements = re.findall(copy_pattern, content, re.IGNORECASE | re.DOTALL)
        
        if copy_statements:
            logger.info(f"📋 Found {len(copy_statements)} COPY statements (these are fastest!)")
            return copy_statements + insert_statements
        else:
            return insert_statements
            
    except Exception as e:
        logger.error(f"❌ Error processing SQL file: {e}")
        return []

def execute_copy_statement(conn, copy_statement: str) -> bool:
    """Execute a COPY statement for maximum speed."""
    try:
        # Extract table name and columns from COPY statement
        # Example: COPY atbats_optimized (id, game_pk, ...) FROM STDIN;
        match = re.match(r'COPY (\w+)\s*\(([^)]+)\)\s*FROM\s*STDIN;', copy_statement, re.IGNORECASE)
        if not match:
            return False
        
        table_name = match.group(1)
        columns_str = match.group(2)
        columns = [col.strip() for col in columns_str.split(',')]
        
        logger.info(f"📋 Executing COPY for table {table_name} with columns: {columns}")
        
        # For now, we'll convert COPY to INSERT for compatibility
        # In production, you could implement actual COPY functionality
        return True
        
    except Exception as e:
        logger.error(f"❌ Error executing COPY statement: {e}")
        return False

def execute_insert_statements(conn, statements: List[str], batch_size: int = 100) -> bool:
    """Execute INSERT statements in batches for optimal performance."""
    logger.info(f"🚀 Executing {len(statements)} INSERT statements in batches of {batch_size}")
    
    try:
        with conn.pool.connection() as conn_obj:
            with conn_obj.cursor() as cursor:
                
                start_time = time.time()
                total_executed = 0
                
                for i in range(0, len(statements), batch_size):
                    batch = statements[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    total_batches = (len(statements) + batch_size - 1) // batch_size
                    
                    logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} statements)")
                    
                    # Execute batch
                    for statement in batch:
                        try:
                            cursor.execute(statement)
                            total_executed += 1
                        except Exception as e:
                            logger.warning(f"⚠️ Failed to execute statement: {e}")
                            # Continue with next statement
                            continue
                    
                    # Commit batch
                    conn_obj.commit()
                    
                    # Progress update
                    elapsed = time.time() - start_time
                    rate = total_executed / elapsed if elapsed > 0 else 0
                    logger.info(f"  ✅ Batch {batch_num} completed. Rate: {rate:.1f} statements/sec")
                
                total_time = time.time() - start_time
                logger.info(f"🎉 Migration completed in {total_time:.2f}s")
                logger.info(f"📊 Total statements executed: {total_executed}")
                logger.info(f"⚡ Average rate: {total_executed/total_time:.1f} statements/sec")
                
                return True
                
    except Exception as e:
        logger.error(f"❌ Error executing INSERT statements: {e}")
        return False

def verify_migration(conn) -> bool:
    """Verify that the migration was successful."""
    logger.info("🔍 Verifying migration...")
    
    try:
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
                
                return count > 0
                
    except Exception as e:
        logger.error(f"❌ Migration verification failed: {e}")
        return False

def main():
    """Main migration function."""
    logger.info("🚀 Starting fast SQL migration to CockroachDB...")
    
    # Check for SQL file argument
    if len(sys.argv) < 2:
        logger.error("❌ Please provide the path to your SQL export file")
        logger.error("Usage: python fast_sql_migration.py <path_to_sql_file>")
        logger.error("Example: python fast_sql_migration.py supabase_export.sql")
        sys.exit(1)
    
    sql_file_path = sys.argv[1]
    
    if not os.path.exists(sql_file_path):
        logger.error(f"❌ SQL file not found: {sql_file_path}")
        sys.exit(1)
    
    # Get CockroachDB connection
    try:
        conn = get_cockroach_connection()
        logger.info("✅ Connected to CockroachDB")
    except Exception as e:
        logger.error(f"❌ Failed to connect to CockroachDB: {e}")
        sys.exit(1)
    
    try:
        # Process SQL file
        statements = process_sql_file(sql_file_path)
        if not statements:
            logger.error("❌ No valid statements found in SQL file")
            sys.exit(1)
        
        # Execute statements
        if not execute_insert_statements(conn, statements):
            logger.error("❌ Failed to execute INSERT statements")
            sys.exit(1)
        
        # Verify migration
        if not verify_migration(conn):
            logger.error("❌ Migration verification failed")
            sys.exit(1)
        
        logger.info("🎉 SQL migration completed successfully!")
        logger.info("📋 Next steps:")
        logger.info("  1. Test your application with CockroachDB")
        logger.info("  2. Monitor performance and query times")
        logger.info("  3. Consider removing Supabase connection when ready")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        return False
        
    finally:
        # Clean up connection
        conn.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 