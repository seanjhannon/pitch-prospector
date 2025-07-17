import sqlite3
from typing import List, Tuple, Dict, Any

DB_PATH = "pitch_prospector/data/pitchprospector.sqlite"

def get_connection():
    return sqlite3.connect(DB_PATH)

def get_atbats_by_date_range(start_date: str, end_date: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM atbats WHERE game_date >= ? AND game_date <= ?
        """,
        (start_date, end_date)
    )
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

def get_atbats_by_sequence_hash(sequence_hash: str) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM atbats WHERE pitch_sequence_hash = ?",
        (sequence_hash,)
    )
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

def get_pitch_sequences_for_atbat(atbat_id: int) -> List[Dict[str, Any]]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM pitch_sequences WHERE atbat_id = ? ORDER BY pitch_order ASC",
        (atbat_id,)
    )
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    conn.close()
    return [dict(zip(columns, row)) for row in rows] 