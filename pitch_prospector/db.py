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