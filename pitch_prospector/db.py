import os
import psycopg2
import json

# --- Connection Setup ---
def get_connection():
    """
    Returns a new connection to the Supabase/Postgres database using environment variables:
    - SUPABASE_DB_HOST
    - SUPABASE_DB_PORT
    - SUPABASE_DB_NAME
    - SUPABASE_DB_USER
    - SUPABASE_DB_PASSWORD
    """
    return psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=os.environ.get("SUPABASE_DB_PORT", 5432),
        dbname=os.environ["SUPABASE_DB_NAME"],
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"]
    )

# --- Table Creation ---
def create_tables():
    """
    Creates the atbats_simple table in the connected Postgres database.
    """
    create_atbats_simple = """
    CREATE TABLE IF NOT EXISTS atbats_simple (
        id SERIAL PRIMARY KEY,
        game_pk BIGINT NOT NULL,
        at_bat_number INTEGER NOT NULL,
        game_date DATE NOT NULL,
        batter BIGINT NOT NULL,
        pitcher BIGINT NOT NULL,
        inning INTEGER NOT NULL,
        pitch_sequence_hash VARCHAR(40) NOT NULL,
        pitch_sequence JSONB,
        pitch_level_data JSONB,
        UNIQUE(game_pk, at_bat_number)
    );
    """
    create_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_pitch_sequence_hash ON atbats_simple(pitch_sequence_hash);",
        "CREATE INDEX IF NOT EXISTS idx_game_date ON atbats_simple(game_date);"
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_atbats_simple)
            for idx in create_indexes:
                cur.execute(idx)
        conn.commit()

# --- Main Entrypoint ---
def main():
    """
    Run this script to create the necessary tables in your Supabase/Postgres DB.
    """
    print("Creating tables in Supabase/Postgres DB...")
    create_tables()
    print("Done.")

if __name__ == "__main__":
    main()

def get_atbats_by_date_range(start_date, end_date):
    """
    Fetch atbats between start_date and end_date (inclusive).
    Returns a list of dicts.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, game_pk, at_bat_number, game_date, batter, pitcher, inning, pitch_sequence_hash
                FROM atbats_simple
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

def get_atbats_by_sequence_hash(sequence_hash):
    """
    Fetch atbats with a specific pitch sequence hash.
    Returns a list of dicts.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, game_pk, at_bat_number, game_date, batter, pitcher, inning, pitch_sequence_hash
                FROM atbats_simple
                WHERE pitch_sequence_hash = %s
                ORDER BY game_date DESC, game_pk DESC, at_bat_number DESC
                """,
                (sequence_hash,)
            )
            columns = [desc[0] for desc in cur.description]
            try:
                rows = cur.fetchall()
            except:
                rows = []
            return [dict(zip(columns, row)) for row in rows]

def get_pitch_sequences_for_atbat(atbat_id):
    """
    Fetch pitch sequence data for a specific at-bat.
    Returns a list of dicts with pitch-level data.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pitch_sequence, pitch_level_data
                FROM atbats_simple
                WHERE id = %s
                """,
                (atbat_id,)
            )
            result = cur.fetchone()
            if result:
                # Supabase JSONB fields are already parsed as Python objects
                pitch_sequence = result[0] if result[0] is not None else []
                pitch_level_data = result[1] if result[1] is not None else []
                
                # Ensure we have valid lists
                if not isinstance(pitch_sequence, list):
                    pitch_sequence = []
                if not isinstance(pitch_level_data, list):
                    pitch_level_data = []
                
                # Combine pitch sequence with pitch level data
                combined_data = []
                for i, (pitch_type, description) in enumerate(pitch_sequence):
                    pitch_data = pitch_level_data[i] if i < len(pitch_level_data) else {}
                    combined_data.append({
                        'pitch_type': pitch_type,
                        'description': description,
                        'release_speed': pitch_data.get('release_speed'),
                        'zone': pitch_data.get('zone'),
                        'pfx_x': pitch_data.get('pfx_x'),
                        'pfx_z': pitch_data.get('pfx_z'),
                        'xc': pitch_data.get('xc'),
                        'yc': pitch_data.get('yc'),
                        'zc': pitch_data.get('zc'),
                        'vx0': pitch_data.get('vx0'),
                        'vy0': pitch_data.get('vy0'),
                        'vz0': pitch_data.get('vz0'),
                        'ax': pitch_data.get('ax'),
                        'ay': pitch_data.get('ay'),
                        'az': pitch_data.get('az'),
                        'break_y': pitch_data.get('break_y'),
                        'break_angle': pitch_data.get('break_angle'),
                        'break_length': pitch_data.get('break_length'),
                        'spin_rate': pitch_data.get('release_spin_rate'),
                        'spin_direction': pitch_data.get('spin_direction')
                    })
                return combined_data
            return []

def insert_atbats(rows):
    """
    Insert atbats into the atbats_simple table.
    """
    if not rows:
        return
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            atbat_data = []
            for row in rows:
                atbat_data.append((
                    row["game_pk"],
                    row["at_bat_number"],
                    row["game_date"],
                    row["batter"],
                    row["pitcher"],
                    row["inning"],
                    row["pitch_sequence_hash"],
                    json.dumps(row.get("pitch_sequence", [])),
                    json.dumps(row.get("pitch_level_data", []))
                ))
            
            cur.executemany("""
                INSERT INTO atbats_simple (
                    game_pk, at_bat_number, game_date, batter, pitcher, inning, 
                    pitch_sequence_hash, pitch_sequence, pitch_level_data
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (game_pk, at_bat_number) DO NOTHING
            """, atbat_data)
            
        conn.commit()

def insert_pitch_sequences(rows):
    """
    This function is no longer needed with the atbats_simple table.
    Pitch sequences are now stored as JSONB in the atbats_simple table.
    """
    pass

def init_db_main(db_path):
    """
    Initialize the database schema.
    This is a no-op function for backward compatibility.
    """
    pass 