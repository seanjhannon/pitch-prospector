"""
CockroachDB configuration for Pitch Prospector migration.
This module works with Streamlit secrets for secure configuration.
"""

import os
import base64
import tempfile
import streamlit as st
from pathlib import Path

def get_cockroach_connection_params():
    """Get connection parameters for CockroachDB from Streamlit secrets."""
    try:
        # Get base DSN from secrets
        base_dsn = st.secrets["crdb"]["dsn"]
        
        # Materialize CA cert to a temp file
        ca_b64 = st.secrets["crdb"]["ca_cert_b64"]
        ca_bytes = base64.b64decode(ca_b64)
        cert_path = os.path.join(tempfile.gettempdir(), "cockroach-root.crt")
        
        with open(cert_path, "wb") as f:
            f.write(ca_bytes)
        
        # Parse the base DSN and add the certificate path
        if "?" in base_dsn:
            dsn = f"{base_dsn}&sslrootcert={cert_path}"
        else:
            dsn = f"{base_dsn}?sslrootcert={cert_path}"
        
        return {
            "dsn": dsn,
            "cert_path": cert_path
        }
        
    except KeyError as e:
        st.error(f"Missing required CockroachDB secret: {e}")
        st.error("Please add 'crdb' section to your .streamlit/secrets.toml with:")
        st.error("  - dsn: base connection string")
        st.error("  - ca_cert_b64: base64-encoded CA certificate")
        st.stop()
    except Exception as e:
        st.error(f"Error configuring CockroachDB connection: {e}")
        st.stop()

def get_cockroach_connection_string():
    """Get the complete connection string with certificate."""
    config = get_cockroach_connection_params()
    return config["dsn"]

def get_cert_path():
    """Get the path to the temporary CA certificate file."""
    config = get_cockroach_connection_params()
    return config["cert_path"]

# For backward compatibility
COCKROACH_CONFIG = {
    "host": "pitches-8229.jxf.gcp-us-west2.cockroachlabs.cloud",
    "port": 26257,
    "database": "defaultdb",
    "user": "sean",
    "password": "_GS_iQHq4ZjjvwA4-VBqcQ",
    "sslmode": "verify-full"
} 