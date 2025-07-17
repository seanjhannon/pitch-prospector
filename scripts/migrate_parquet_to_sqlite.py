import sqlite3
import glob
import pandas as pd
import os

DB_PATH = "pitch_prospector/data/pitchprospector.sqlite"
PARQUET_GLOB = "pitch_prospector/data/atbat_pitch_sequence_index_*.parquet"


def migrate():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    files = sorted(glob.glob(PARQUET_GLOB))
    print(f"Found {len(files)} Parquet files to migrate.")
    for f in files:
        print(f"Processing {f} ...")
        df = pd.read_parquet(f)
        for _, row in df.iterrows():
            # Insert atbat
            try:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO atbats (game_pk, at_bat_number, game_date, batter, pitcher, inning, pitch_sequence_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["game_pk"],
                        row["at_bat_number"],
                        str(row["game_date"].date()) if hasattr(row["game_date"], "date") else str(row["game_date"]),
                        row["batter"],
                        row["pitcher"],
                        row["inning"],
                        row["pitch_sequence_hash"]
                    )
                )
                cur.execute("SELECT id FROM atbats WHERE game_pk=? AND at_bat_number=?", (row["game_pk"], row["at_bat_number"]))
                atbat_id = cur.fetchone()[0]
                # Insert pitch sequence
                for i, pitch in enumerate(row["pitch_sequence"]):
                    pitch_type, description = pitch
                    pitch_level_data = row["pitch_level_data"][i]
                    cur.execute(
                        """
                        INSERT INTO pitch_sequences (atbat_id, pitch_order, pitch_type, description, release_speed, zone)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            atbat_id,
                            i,
                            pitch_type,
                            description,
                            pitch_level_data.get("release_speed"),
                            pitch_level_data.get("zone")
                        )
                    )
            except Exception as e:
                print(f"❌ Error processing atbat {row['game_pk']}-{row['at_bat_number']}: {e}")
        conn.commit()
    print("✅ Migration complete!")
    conn.close()

if __name__ == "__main__":
    migrate() 