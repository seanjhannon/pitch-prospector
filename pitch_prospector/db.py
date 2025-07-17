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

def init_db_main(db_path=None):
    """Initialize the SQLite schema for atbats and pitch_sequences tables."""
    if db_path is None:
        db_path = DB_PATH
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS atbats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_pk INTEGER,
            at_bat_number INTEGER,
            game_date DATE,
            batter INTEGER,
            pitcher INTEGER,
            inning INTEGER,
            pitch_sequence_hash VARCHAR(40),
            UNIQUE(game_pk, at_bat_number)
        );
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS pitch_sequences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            atbat_id INTEGER REFERENCES atbats(id),
            pitch_order INTEGER,
            pitch_type VARCHAR(10),
            description VARCHAR(50),
            release_speed DECIMAL(4,1),
            zone INTEGER
        );
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_pitch_sequence_hash ON atbats(pitch_sequence_hash);')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_game_date ON atbats(game_date);')
    conn.commit()
    conn.close() 