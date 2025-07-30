"""
Database connection pool for improved performance.
Uses psycopg2.pool for connection pooling.
"""

import os
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
import threading

# Global connection pool
_connection_pool = None
_pool_lock = threading.Lock()

def get_connection_pool():
    """Get or create the connection pool."""
    global _connection_pool
    
    if _connection_pool is None:
        with _pool_lock:
            if _connection_pool is None:
                _connection_pool = pool.SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,
                    host=os.environ["SUPABASE_DB_HOST"],
                    port=os.environ.get("SUPABASE_DB_PORT", 5432),
                    dbname=os.environ["SUPABASE_DB_NAME"],
                    user=os.environ["SUPABASE_DB_USER"],
                    password=os.environ["SUPABASE_DB_PASSWORD"]
                )
    
    return _connection_pool

@contextmanager
def get_db_connection():
    """Get a database connection from the pool."""
    pool = get_connection_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)

def close_connection_pool():
    """Close the connection pool."""
    global _connection_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None 