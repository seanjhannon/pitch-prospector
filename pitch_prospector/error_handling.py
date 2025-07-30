"""
Error handling utilities for the Pitch Prospector application.
Provides consistent error handling and user-friendly error messages.
"""

import logging
import streamlit as st
from typing import Optional, Any, Callable
from functools import wraps

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PitchProspectorError(Exception):
    """Base exception for Pitch Prospector application."""
    pass

class DatabaseError(PitchProspectorError):
    """Database-related errors."""
    pass

class DataProcessingError(PitchProspectorError):
    """Data processing errors."""
    pass

class ValidationError(PitchProspectorError):
    """Input validation errors."""
    pass

def handle_database_errors(func: Callable) -> Callable:
    """Decorator to handle database errors gracefully."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Database error in {func.__name__}: {e}")
            st.error(f"Database connection error. Please try again later.")
            return None
    return wrapper

def handle_player_lookup_errors(func: Callable) -> Callable:
    """Decorator to handle player lookup errors gracefully."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Player lookup error in {func.__name__}: {e}")
            # Return empty dict instead of failing
            return {}
    return wrapper

def validate_date_range(start_date, end_date) -> bool:
    """Validate date range inputs."""
    if start_date > end_date:
        st.error("Start date must be before end date.")
        return False
    
    if start_date.year < 2015:
        st.error("Start date must be 2015 or later (Statcast data availability).")
        return False
    
    return True

def validate_pitch_sequence(pitch_inputs, outcome_inputs) -> bool:
    """Validate pitch sequence inputs."""
    if not pitch_inputs or not outcome_inputs:
        st.error("Please select at least one pitch.")
        return False
    
    if len(pitch_inputs) != len(outcome_inputs):
        st.error("Pitch types and outcomes must match.")
        return False
    
    return True

def safe_int_conversion(value: Any, default: int = 0) -> int:
    """Safely convert value to integer with fallback."""
    try:
        return int(value) if value is not None else default
    except (ValueError, TypeError):
        return default

def safe_str_conversion(value: Any, default: str = "") -> str:
    """Safely convert value to string with fallback."""
    try:
        return str(value) if value is not None else default
    except (ValueError, TypeError):
        return default

def log_and_display_error(error: Exception, context: str = ""):
    """Log error and display user-friendly message."""
    logger.error(f"{context}: {error}")
    
    if isinstance(error, ValidationError):
        st.error(str(error))
    elif isinstance(error, DatabaseError):
        st.error("Database error occurred. Please try again later.")
    elif isinstance(error, DataProcessingError):
        st.error("Data processing error. Please try again.")
    else:
        st.error("An unexpected error occurred. Please try again later.") 