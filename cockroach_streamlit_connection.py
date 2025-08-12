"""
Streamlit-compatible CockroachDB connection module for Pitch Prospector.
Uses psycopg and connection pooling with temporary CA certificate files.
"""

import os
import base64
import tempfile
import streamlit as st
import psycopg
from psycopg_pool import ConnectionPool
from typing import Optional, List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class CockroachDBConnection:
    """
    Streamlit-compatible CockroachDB connection class.
    Mimics the interface of st-supabase-connection for easy migration.
    """
    
    def __init__(self):
        self.pool = self._create_connection_pool()
        self._cert_path = None
    
    def _write_ca_from_secrets(self) -> str:
        """Materialize CA cert to a temp file from Streamlit secrets."""
        try:
            ca_b64 = st.secrets["crdb"]["ca_cert_b64"]
            ca_bytes = base64.b64decode(ca_b64)
            cert_path = os.path.join(tempfile.gettempdir(), "cockroach-root.crt")
            
            with open(cert_path, "wb") as f:
                f.write(ca_bytes)
            
            logger.info(f"✅ CA certificate written to: {cert_path}")
            return cert_path
            
        except KeyError as e:
            st.error(f"Missing required CockroachDB secret: {e}")
            st.error("Please add 'crdb' section to your .streamlit/secrets.toml with:")
            st.error("  - dsn: base connection string")
            st.error("  - ca_cert_b64: base64-encoded CA certificate")
            st.stop()
        except Exception as e:
            st.error(f"Error writing CA certificate: {e}")
            st.stop()
    
    def _create_connection_pool(self) -> ConnectionPool:
        """Create a connection pool for CockroachDB."""
        try:
            # Get base DSN from secrets
            base_dsn = st.secrets["crdb"]["dsn"]
            
            # Materialize CA cert to a temp file
            self._cert_path = self._write_ca_from_secrets()
            
            # Build DSN with sslrootcert=<temp file> and keep sslmode=verify-full
            if "?" in base_dsn:
                dsn = f"{base_dsn}&sslrootcert={self._cert_path}"
            else:
                dsn = f"{base_dsn}?sslrootcert={self._cert_path}"
            
            logger.info(f"🔗 Connecting to CockroachDB with DSN: {dsn[:50]}...")
            
            # Use a small pool so first request is warm
            pool = ConnectionPool(dsn, min_size=1, max_size=5)
            
            # Warm the pool on import
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    logger.info("✅ Connection pool warmed successfully")
            
            return pool
            
        except Exception as e:
            st.error(f"Failed to create CockroachDB connection pool: {e}")
            st.stop()
    
    def table(self, table_name: str):
        """Return a table object for querying."""
        return CockroachDBTable(self, table_name)
    
    def close(self):
        """Close the connection pool."""
        if self.pool:
            self.pool.close()
        
        # Clean up temporary certificate file
        if self._cert_path and os.path.exists(self._cert_path):
            try:
                os.remove(self._cert_path)
                logger.info(f"🧹 Cleaned up temporary certificate: {self._cert_path}")
            except Exception as e:
                logger.warning(f"⚠️ Failed to clean up certificate: {e}")

class CockroachDBTable:
    """
    Table interface for CockroachDB queries.
    Mimics the Supabase table interface.
    """
    
    def __init__(self, connection: CockroachDBConnection, table_name: str):
        self.connection = connection
        self.table_name = table_name
        self._select_fields = "*"
        self._where_conditions = []
        self._order_by = None
        self._limit_value = None
        self._offset_value = None
        self._count_exact = False
    
    def select(self, *fields, count: Optional[str] = None):
        """Select fields from the table."""
        if fields:
            self._select_fields = ", ".join(fields)
        if count == "exact":
            self._count_exact = True
        return self
    
    def eq(self, field: str, value: Any):
        """Add equality condition."""
        self._where_conditions.append((field, "=", value))
        return self
    
    def ne(self, field: str, value: Any):
        """Add inequality condition."""
        self._where_conditions.append((field, "!=", value))
        return self
    
    def gt(self, field: str, value: Any):
        """Add greater than condition."""
        self._where_conditions.append((field, ">", value))
        return self
    
    def gte(self, field: str, value: Any):
        """Add greater than or equal condition."""
        self._where_conditions.append((field, ">=", value))
        return self
    
    def lt(self, field: str, value: Any):
        """Add less than condition."""
        self._where_conditions.append((field, "<", value))
        return self
    
    def lte(self, field: str, value: Any):
        """Add less than or equal condition."""
        self._where_conditions.append((field, "<=", value))
        return self
    
    def order(self, field: str, desc: bool = False):
        """Add ordering."""
        direction = "DESC" if desc else "ASC"
        self._order_by = f"{field} {direction}"
        return self
    
    def limit(self, value: int):
        """Add limit."""
        self._limit_value = value
        return self
    
    def execute(self):
        """Execute the query and return results."""
        try:
            with self.connection.pool.connection() as conn:
                with conn.cursor() as cursor:
                    # Build the SQL query
                    sql_parts = [f"SELECT {self._select_fields} FROM {self.table_name}"]
                    
                    # Add WHERE conditions
                    params = []
                    if self._where_conditions:
                        where_clauses = []
                        for field, operator, value in self._where_conditions:
                            where_clauses.append(f"{field} {operator} %s")
                            params.append(value)
                        sql_parts.append(f"WHERE {' AND '.join(where_clauses)}")
                    
                    # Add ORDER BY
                    if self._order_by:
                        sql_parts.append(f"ORDER BY {self._order_by}")
                    
                    # Add LIMIT
                    if self._limit_value is not None:
                        sql_parts.append(f"LIMIT {self._limit_value}")
                    
                    # Add OFFSET
                    if self._offset_value is not None:
                        sql_parts.append(f"OFFSET {self._offset_value}")
                    
                    sql = " ".join(sql_parts)
                    
                    # Execute query
                    if self._count_exact:
                        # For count queries, we need to get the actual count
                        count_sql = f"SELECT COUNT(*) FROM {self.table_name}"
                        if self._where_conditions:
                            where_clauses = []
                            for field, operator, value in self._where_conditions:
                                where_clauses.append(f"{field} {operator} %s")
                            count_sql += f" WHERE {' AND '.join(where_clauses)}"
                        
                        cursor.execute(count_sql, params)
                        count_result = cursor.fetchone()
                        count = count_result[0] if count_result else 0
                        
                        # Now execute the actual query
                        cursor.execute(sql, params)
                        data = cursor.fetchall()
                        
                        # Create a result object that mimics Supabase response
                        class MockResult:
                            def __init__(self, data, count):
                                self.data = data
                                self.count = count
                        
                        return MockResult(data, count)
                    else:
                        cursor.execute(sql, params)
                        data = cursor.fetchall()
                        
                        # Create a result object that mimics Supabase response
                        class MockResult:
                            def __init__(self, data):
                                self.data = data
                                self.count = len(data)
                        
                        return MockResult(data)
                        
        except Exception as e:
            logger.error(f"❌ Query execution failed: {e}")
            raise

def get_cockroach_connection():
    """Get a CockroachDB connection instance."""
    return CockroachDBConnection()

# Streamlit connection interface
@st.cache_resource
def get_connection():
    """Get a cached CockroachDB connection for Streamlit."""
    return get_cockroach_connection()

# For backward compatibility with existing code
def connection(name: str, type: str = None):
    """Streamlit connection interface that returns CockroachDB connection."""
    if type == "cockroach" or name == "cockroach":
        return get_cockroach_connection()
    else:
        # Fallback to CockroachDB for now
        return get_cockroach_connection()

# Example usage:
# conn = st.connection("cockroach", type="cockroach")
# result = conn.table("atbats_optimized").select("id", "game_date").limit(10).execute()
# print(f"Found {result.count} records")
# for row in result.data:
#     print(row) 