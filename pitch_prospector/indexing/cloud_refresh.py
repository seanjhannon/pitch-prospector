"""
Cloud-friendly data refresh that doesn't write to filesystem
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from pybaseball import statcast
import sqlite3
import warnings
from pitch_prospector.db import DB_PATH
from pitch_prospector.indexing.pitch_index import process_file

warnings.filterwarnings(
    "ignore",
    message=".*errors='ignore' is deprecated.*",
    category=FutureWarning,
    module="pybaseball.datahelpers.postprocessing"
)

def get_latest_game_date():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT MAX(game_date) FROM atbats")
    result = cur.fetchone()[0]
    conn.close()
    return result

def insert_new_data_from_indexed_rows(rows):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    inserted = 0
    for row in rows:
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
            inserted += 1
        except Exception as e:
            print(f"❌ Error processing atbat {row['game_pk']}-{row['at_bat_number']}: {e}")
    conn.commit()
    conn.close()
    return inserted

def refresh_sqlite_db():
    """On-demand refresh: fetch and insert new data into SQLite if needed."""
    if 'last_sqlite_refresh' in st.session_state:
        last_refresh = st.session_state['last_sqlite_refresh']
        if (datetime.now() - last_refresh).seconds < 3600:
            st.info("Refresh throttled: Try again later.")
            return
    latest_date = get_latest_game_date()
    if latest_date is None:
        st.warning("No data in DB. Please run a full migration first.")
        return
    start_date = pd.to_datetime(latest_date) + pd.Timedelta(days=1)
    end_date = datetime.now()
    if start_date > end_date:
        st.success("Database is already up to date!")
        return
    with st.spinner(f"Fetching Statcast data from {start_date.date()} to {end_date.date()}..."):
        df = statcast(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
        if df.empty:
            st.info("No new data available from Statcast.")
            st.session_state['last_sqlite_refresh'] = datetime.now()
            return
        # Index in-memory DataFrame
        indexed_rows = process_file(df)
        inserted = insert_new_data_from_indexed_rows(indexed_rows)
        st.success(f"Inserted {inserted} new at-bats into the database!")
        st.session_state['last_sqlite_refresh'] = datetime.now() 