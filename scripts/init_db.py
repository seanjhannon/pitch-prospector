import sqlite3

SCHEMA = '''
CREATE TABLE IF NOT EXISTS atbats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_pk INTEGER,
    at_bat_number INTEGER,
    game_date DATE,
    batter INTEGER,
    pitcher INTEGER,
    inning INTEGER,
    pitch_sequence_hash TEXT,
    UNIQUE(game_pk, at_bat_number)
);

CREATE TABLE IF NOT EXISTS pitch_sequences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    atbat_id INTEGER REFERENCES atbats(id),
    pitch_order INTEGER,
    pitch_type TEXT,
    description TEXT,
    release_speed REAL,
    zone INTEGER
);

CREATE INDEX IF NOT EXISTS idx_pitch_sequence_hash ON atbats(pitch_sequence_hash);
CREATE INDEX IF NOT EXISTS idx_game_date ON atbats(game_date);
'''

def main(db_path="pitch_prospector/data/pitchprospector.sqlite"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executescript(SCHEMA)
    conn.commit()
    print(f"✅ Initialized DB at {db_path}")
    conn.close()

if __name__ == "__main__":
    main() 