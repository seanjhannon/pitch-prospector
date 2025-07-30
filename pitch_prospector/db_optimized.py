"""
Optimized database functions for the atbats_optimized table.
This table stores only mission-critical columns to minimize storage usage.
"""

import os
import psycopg2
import json
from typing import List, Dict, Any, Optional
from .db_pool import get_db_connection

def get_atbats_by_date_range_optimized(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """
    Fetch at-bats between start_date and end_date (inclusive).
    Returns a list of dicts from the optimized table.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, game_pk, at_bat_number, game_date, batter, pitcher, inning,
                       pitch_sequence, pitch_data
                FROM atbats_optimized
                WHERE game_date BETWEEN %s AND %s
                ORDER BY game_date DESC, game_pk DESC, at_bat_number DESC
                """,
                (start_date, end_date)
            )
            columns = [desc[0] for desc in cur.description]
            try:
                rows = cur.fetchall()
            except:
                rows = []
            return [dict(zip(columns, row)) for row in rows]

def get_atbats_by_sequence_optimized(pitch_sequence: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch at-bats with a specific pitch sequence.
    Returns a list of dicts from the optimized table.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, game_pk, at_bat_number, game_date, batter, pitcher, inning,
                       pitch_sequence, pitch_data
                FROM atbats_optimized
                WHERE pitch_sequence = %s::jsonb
                ORDER BY game_date DESC, game_pk DESC, at_bat_number DESC
                """,
                (json.dumps(pitch_sequence),)
            )
            columns = [desc[0] for desc in cur.description]
            try:
                rows = cur.fetchall()
            except:
                rows = []
            return [dict(zip(columns, row)) for row in rows]

def get_atbats_by_date_and_sequence_optimized(start_date: str, end_date: str, pitch_sequence: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch at-bats with specific date range and pitch sequence.
    This is the optimized version that uses database-level filtering.
    Returns a list of dicts from the optimized table.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, game_pk, at_bat_number, game_date, batter, pitcher, inning,
                       pitch_sequence, pitch_data
                FROM atbats_optimized
                WHERE game_date BETWEEN %s AND %s AND pitch_sequence = %s::jsonb
                ORDER BY game_date DESC, game_pk DESC, at_bat_number DESC
                """,
                (start_date, end_date, json.dumps(pitch_sequence))
            )
            columns = [desc[0] for desc in cur.description]
            try:
                rows = cur.fetchall()
            except:
                rows = []
            return [dict(zip(columns, row)) for row in rows]

def get_pitch_data_for_atbat_optimized(atbat_id: int) -> List[Dict[str, Any]]:
    """
    Fetch pitch data for a specific at-bat from the optimized table.
    Returns a list of dicts with pitch-level data in the format expected by the UI.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pitch_sequence, pitch_data
                FROM atbats_optimized
                WHERE id = %s
                """,
                (atbat_id,)
            )
            result = cur.fetchone()
            if result:
                # Parse JSONB fields
                pitch_sequence = result[0] if result[0] is not None else []
                pitch_data = result[1] if result[1] is not None else []
                
                # Ensure we have valid lists
                if not isinstance(pitch_sequence, list):
                    pitch_sequence = []
                if not isinstance(pitch_data, list):
                    pitch_data = []
                
                # Combine pitch sequence with pitch data for UI display
                combined_data = []
                for i, pitch_type in enumerate(pitch_sequence):
                    # Get corresponding pitch data (speed and zone)
                    speed, zone = pitch_data[i] if i < len(pitch_data) else [0, 0]
                    
                    combined_data.append({
                        'pitch_type': pitch_type,
                        'description': 'unknown',  # Default since we don't store descriptions
                        'release_speed': speed,
                        'zone': zone
                    })
                return combined_data
            return []

def insert_atbats_optimized(atbats: List[Dict[str, Any]]):
    """
    Insert at-bats into the atbats_optimized table.
    """
    if not atbats:
        return
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Prepare data for bulk insert
            data = []
            for atbat in atbats:
                data.append((
                    atbat["game_pk"],
                    atbat["at_bat_number"],
                    atbat["game_date"],
                    atbat["batter"],
                    atbat["pitcher"],
                    atbat["inning"],
                    json.dumps(atbat["pitch_sequence"]),
                    json.dumps(atbat["pitch_data"])
                ))
            
            # Bulk insert
            cur.executemany("""
                INSERT INTO atbats_optimized (
                    game_pk, at_bat_number, game_date, batter, pitcher, inning,
                    pitch_sequence, pitch_data
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_pk, at_bat_number) DO NOTHING
            """, data)
            
            conn.commit()

def get_table_stats_optimized() -> Dict[str, Any]:
    """
    Get statistics about the optimized table.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Get record count and table size
            cur.execute("""
                SELECT 
                    COUNT(*) as record_count,
                    pg_size_pretty(pg_total_relation_size('atbats_optimized')) as table_size,
                    pg_total_relation_size('atbats_optimized') as size_bytes
                FROM atbats_optimized
            """)
            
            result = cur.fetchone()
            if result:
                record_count, table_size, size_bytes = result
                return {
                    'record_count': record_count,
                    'table_size': table_size,
                    'size_bytes': size_bytes,
                    'size_per_record': size_bytes / record_count if record_count > 0 else 0
                }
            return {}

def check_optimized_table_exists() -> bool:
    """
    Check if the optimized table exists.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'atbats_optimized'
                )
            """)
            result = cur.fetchone()
            return result[0] if result else False 