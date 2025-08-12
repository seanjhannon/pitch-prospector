"""
Streamlit Cloud compatible CockroachDB connection module.
Uses base64 certificate directly in connection string for cloud deployment.
"""

import streamlit as st
import psycopg
from typing import Optional, List, Dict, Any
import logging
import base64

logger = logging.getLogger(__name__)

class CockroachDBCloudConnection:
    """
    Streamlit Cloud compatible CockroachDB connection class.
    Embeds CA certificate directly in connection string.
    """
    
    def __init__(self):
        self.dsn = self._build_connection_string()
    
    def _build_connection_string(self) -> str:
        """Build the connection string with embedded certificate."""
        try:
            # Get base DSN from secrets
            base_dsn = st.secrets["crdb"]["dsn"]
            ca_cert_b64 = st.secrets["crdb"]["ca_cert_b64"]
            
            # Decode certificate
            ca_cert = base64.b64decode(ca_cert_b64).decode('utf-8')
            
            # Build DSN with embedded certificate (no temp files)
            if "?" in base_dsn:
                dsn = f"{base_dsn}&sslcert=&sslkey=&sslrootcert={ca_cert}"
            else:
                dsn = f"{base_dsn}?sslcert=&sslkey=&sslrootcert={ca_cert}"
            
            logger.info(f"🔗 Built CockroachDB connection string with embedded certificate")
            return dsn
            
        except Exception as e:
            st.error(f"Failed to build CockroachDB connection string: {e}")
            st.stop()
    
    def table(self, table_name: str):
        """Return a table object for querying."""
        return CockroachDBCloudTable(self, table_name)
    
    def close(self):
        """No pool to close, but keeping interface consistent."""
        pass

class CockroachDBCloudTable:
    """
    Table interface for CockroachDB queries.
    Mimics the Supabase table interface exactly.
    """
    
    def __init__(self, connection: CockroachDBCloudConnection, table_name: str):
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
            # Create a new connection for each query (simple approach)
            with psycopg.connect(self.connection.dsn) as conn:
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
                        
                        # Create a result object that mimics Supabase response exactly
                        class MockResult:
                            def __init__(self, data, count):
                                self.data = data
                                self.count = count
                        
                        return MockResult(data, count)
                    else:
                        cursor.execute(sql, params)
                        data = cursor.fetchall()
                        
                        # Create a result object that mimics Supabase response exactly
                        class MockResult:
                            def __init__(self, data):
                                self.data = data
                                self.count = len(data)
                        
                        return MockResult(data)
                        
        except Exception as e:
            logger.error(f"❌ Query execution failed: {e}")
            raise

def get_cockroach_cloud_connection():
    """Get a CockroachDB connection instance for Streamlit Cloud."""
    return CockroachDBCloudConnection()

# Streamlit connection interface
@st.cache_resource
def get_connection():
    """Get a cached CockroachDB connection for Streamlit."""
    return get_cockroach_cloud_connection()

# For backward compatibility with existing code
def connection(name: str, type: str = None):
    """Streamlit connection interface that returns CockroachDB connection."""
    if type == "cockroach" or name == "cockroach":
        return get_cockroach_cloud_connection()
    else:
        # Fallback to CockroachDB for now
        return get_cockroach_cloud_connection() 