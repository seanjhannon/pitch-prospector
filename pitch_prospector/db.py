import os
import psycopg2

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
    Creates the atbats and pitch_sequences tables in the connected Postgres database.
    """
    create_atbats = """
    CREATE TABLE IF NOT EXISTS atbats (
        id SERIAL PRIMARY KEY,
        game_pk INTEGER,
        at_bat_number INTEGER,
        game_date DATE,
        batter BIGINT,
        pitcher BIGINT,
        inning INTEGER,
        pitch_sequence_hash VARCHAR(40),
        UNIQUE(game_pk, at_bat_number)
    );
    """
    create_pitch_sequences = """
    CREATE TABLE IF NOT EXISTS pitch_sequences (
        id SERIAL PRIMARY KEY,
        atbat_id INTEGER REFERENCES atbats(id),
        pitch_order INTEGER,
        pitch_type VARCHAR(10),
        description VARCHAR(50),
        release_speed DECIMAL(4,1),
        zone INTEGER
    );
    """
    create_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_pitch_sequence_hash ON atbats(pitch_sequence_hash);",
        "CREATE INDEX IF NOT EXISTS idx_game_date ON atbats(game_date);"
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(create_atbats)
            cur.execute(create_pitch_sequences)
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

# TODO: Add data access/query functions for the app
# TODO: Add insert/update helpers for atbats and pitch_sequences 

def get_atbats_by_date_range(start_date, end_date):
    """
    Fetch atbats between start_date and end_date (inclusive).
    Returns a list of dicts.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM atbats WHERE game_date >= %s AND game_date <= %s
                """,
                (start_date, end_date)
            )
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

def get_atbats_by_sequence_hash(sequence_hash):
    """
    Fetch atbats with a given pitch_sequence_hash.
    Returns a list of dicts.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM atbats WHERE pitch_sequence_hash = %s",
                (sequence_hash,)
            )
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

def get_pitch_sequences_for_atbat(atbat_id):
    """
    Fetch pitch sequences for a given atbat_id, ordered by pitch_order.
    Returns a list of dicts.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM pitch_sequences WHERE atbat_id = %s ORDER BY pitch_order ASC",
                (atbat_id,)
            )
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

def insert_atbats(rows):
    """
    Insert multiple atbat rows (list of dicts) into the atbats table.
    Ignores duplicates based on the unique constraint.
    """
    if not rows:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO atbats (game_pk, at_bat_number, game_date, batter, pitcher, inning, pitch_sequence_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_pk, at_bat_number) DO NOTHING
                    """,
                    (
                        row["game_pk"],
                        row["at_bat_number"],
                        row["game_date"],
                        row["batter"],
                        row["pitcher"],
                        row["inning"],
                        row["pitch_sequence_hash"]
                    )
                )
        conn.commit()
    return len(rows)

def insert_pitch_sequences(rows):
    """
    Insert multiple pitch_sequence rows (list of dicts) into the pitch_sequences table.
    """
    if not rows:
        return 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO pitch_sequences (atbat_id, pitch_order, pitch_type, description, release_speed, zone)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row["atbat_id"],
                        row["pitch_order"],
                        row["pitch_type"],
                        row["description"],
                        row.get("release_speed"),
                        row.get("zone")
                    )
                )
        conn.commit()
    return len(rows) 