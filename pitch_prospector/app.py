import streamlit as st
import pandas as pd
import os
import hashlib
from datetime import datetime, timedelta
from pybaseball import playerid_reverse_lookup
import numpy as np

from pitch_prospector.db import get_atbats_by_date_range, get_atbats_by_sequence_hash, get_pitch_sequences_for_atbat

DB_PATH = "pitch_prospector/data/pitchprospector.sqlite"

def ensure_db_initialized():
    from scripts.init_db import main as init_db_main
    with st.spinner("Ensuring database schema..."):
        init_db_main(DB_PATH)
    if not os.path.exists(DB_PATH):
        st.success("Database initialized! Use the refresh button to load data.")

ensure_db_initialized()

from pitch_prospector.indexing.cloud_refresh import refresh_sqlite_db

# Automatically refresh data if DB is empty (last 7 days)
today = datetime.today()
recent_records = get_atbats_by_date_range(str(today.date() - timedelta(days=7)), str(today.date()))
if not recent_records:
    with st.spinner("Loading data from Statcast for first use..."):
        refresh_sqlite_db()

if st.button("Refresh Data from Statcast"):
    refresh_sqlite_db()

st.title("At-Bat Sequence Finder")
st.markdown("Pick a date range to filter historical at-bats.")

from datetime import datetime, timedelta

today = datetime.today()
two_years_ago = today.replace(year=today.year - 2)

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input(
        "Start date",
        value=two_years_ago,
        min_value=datetime(2015, 1, 1),
        max_value=today
    )
with col2:
    end_date = st.date_input(
        "End date",
        value=today,
        min_value=start_date,
        max_value=today
    )

start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

atbat_records = get_atbats_by_date_range(str(start_date.date()), str(end_date.date()))
if not atbat_records:
    st.error("No at-bats found for the selected date range.")
    st.stop()

st.markdown("Pick a pitch sequence to find matching historical at-bats.")

PITCH_TYPE_MAP = {
    "AB": "Automatic Ball",
    "AS": "Automatic Strike",
    "IN": "Intentional Ball",
    "PO": "Pitchout",
    "CS": "Slow Curve (CS)",
    "CH": "Changeup (CH)",
    "CU": "Curveball (CU)",
    "FC": "Cutter (FC)",
    "EP": "Eephus (EP)",
    "FO": "Forkball (FO)",
    "FA": "Four-Seam Fastball (FA)",
    "FF": "Four-Seam Fastball (FF)",
    "KN": "Knuckleball (KN)",
    "KC": "Knuckle-curve (KC)",
    "SC": "Screwball (SC)",
    "SI": "Sinker (SI)",
    "SL": "Slider (SL)",
    "SV": "Slurve (SV)",
    "FS": "Splitter (FS)",
    "ST": "Sweeper (ST)"
}

ALL_OUTCOMES = [
    "ball", "called_strike", "foul", "hit_by_pitch", "missed_bunt",
    "pitchout", "swinging_strike", "swinging_strike_blocked",
    "blocked_ball", "foul_tip", "bunt_foul_tip", "bunt_foul",
    "bunt_miss", "hit_into_play", "hit_into_play_score", "hit_into_play_no_out"
]

all_pitches = sorted(PITCH_TYPE_MAP.keys())
all_outcomes = sorted(ALL_OUTCOMES)

with st.form("pitch_sequence_form"):
    num_pitches = st.number_input("Number of pitches in sequence", min_value=1, max_value=10, value=3)
    pitch_inputs = []
    outcome_inputs = []

    for i in range(num_pitches):
        cols = st.columns([1, 1])
        with cols[0]:
            pitch = st.selectbox(
                f"Pitch {i+1} type",
                all_pitches,
                key=f"pitch_{i}",
                index=all_pitches.index("FF") if "FF" in all_pitches else 0,
                format_func=lambda x: str(PITCH_TYPE_MAP.get(x, x))
            )
        with cols[1]:
            default_strikes = [o for o in all_outcomes if "strike" in o.lower()]
            default_index = all_outcomes.index(default_strikes[0]) if default_strikes else 0
            outcome = st.selectbox(
                f"Pitch {i+1} result",
                all_outcomes,
                key=f"outcome_{i}",
                index=default_index,
                format_func=lambda x: str(x).title().replace('_', ' ')
            )
        pitch_inputs.append(pitch)
        outcome_inputs.append(outcome)

    submitted = st.form_submit_button("Search")

if submitted:
    with st.spinner("Searching for matching at-bats..."):
        sequence = tuple((p, o) for p, o in zip(pitch_inputs, outcome_inputs))
        hash_input = str(sequence).encode("utf-8")
        sequence_hash = hashlib.sha1(hash_input).hexdigest()

        matches = get_atbats_by_sequence_hash(sequence_hash)

        if matches:
            all_ids = set()
            for row in matches:
                all_ids.add(row["pitcher"])
                all_ids.add(row["batter"])
            lookup_df = playerid_reverse_lookup(list(all_ids))
            lookup_df["full_name"] = lookup_df["name_first"] + " " + lookup_df["name_last"]
            lookup_df["key_mlbam"] = lookup_df["key_mlbam"].astype(str)
            name_lookup = lookup_df.set_index("key_mlbam")["full_name"]

            for row in matches:
                row["pitcher_name"] = name_lookup.get(str(row["pitcher"]), str(row["pitcher"]))
                row["batter_name"] = name_lookup.get(str(row["batter"]), str(row["batter"]))
                row["pitcher_img"] = f"https://securea.mlb.com/mlb/images/players/head_shot/{row['pitcher']}.jpg"
                row["batter_img"] = f"https://securea.mlb.com/mlb/images/players/head_shot/{row['batter']}.jpg"

                def build_statcast_url(row):
                    game_date_str = pd.to_datetime(row['game_date']).date()
                    return (
                        f"https://baseballsavant.mlb.com/statcast_search?"
                        f"player_type=pitcher&"
                        f"game_date_gt={game_date_str}&"
                        f"game_date_lt={game_date_str}&"
                        f"pitchers_lookup%5B%5D={row['pitcher']}&"
                        f"batters_lookup%5B%5D={row['batter']}&"
                        f"hfInn={row['inning']}%7C&"
                        f"hfSea={pd.to_datetime(row['game_date']).year}%7C"
                    )

                row["statcast_url"] = build_statcast_url(row)

                pitch_level_data = get_pitch_sequences_for_atbat(row["id"])

                st.markdown(
                    f"<div style='text-align: center;'>"
                    f"<h3>{row['pitcher_name'].title()} vs {row['batter_name'].title()} — {pd.to_datetime(row['game_date']):%B %d, %Y}</h3>"
                    f"</div>",
                    unsafe_allow_html=True
                )
                cols = st.columns([1, 6, 1])
                with cols[0]:
                    st.image(row["pitcher_img"], width=75)
                with cols[1]:
                    pitch_cols = st.columns(len(pitch_level_data))
                    for i, pitch in enumerate(pitch_level_data):
                        with pitch_cols[i]:
                            st.markdown(f"<div style='text-align:center;'>"
                                        f"<strong>{pitch['pitch_type']}</strong><br>"
                                        f"{pitch['release_speed']} mph<br>"
                                        f"Zone {int(pitch.get('zone', '–'))}"
                                        f"</div>", unsafe_allow_html=True)
                with cols[2]:
                    st.image(row["batter_img"], width=75)

                st.markdown(f"<div style='text-align: center;'><a href='{row['statcast_url']}' target='_blank'>🔗 Watch on Statcast</a></div>", unsafe_allow_html=True)
                st.markdown("---")
        else:
            st.subheader("No matching at-bats found.")
