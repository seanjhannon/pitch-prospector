import streamlit as st
import pandas as pd
import os
import hashlib
from datetime import datetime
import pyarrow.parquet as pq
from pybaseball import playerid_reverse_lookup
import numpy as np

# Refresh the data
from indexing.auto_refresh_pitch_index import auto_refresh_pitch_index

# Trigger auto-refresh when app starts (can limit to once per session)
@st.cache_resource
def refresh_data():
    auto_refresh_pitch_index()

refresh_data()  # runs once per app session

INDEX_PATH = "data/atbat_pitch_sequence_index.parquet"
SEASON_INDEX_PATTERN = "data/atbat_pitch_sequence_index_{year}.parquet"

st.title("At-Bat Sequence Finder")
st.markdown("Pick a date range to filter historical at-bats.")

# Date range selector
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


# Make sure they're both converted to datetime64 for filtering
start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)


filters = [("game_date", ">=", start_date), ("game_date", "<=", end_date)]

# Determine which years are needed
start_year = start_date.year
end_year = end_date.year
season_dfs = []
season_files_found = False
for year in range(start_year, end_year + 1):
    season_path = SEASON_INDEX_PATTERN.format(year=year)
    if os.path.exists(season_path):
        df = pq.read_table(season_path).to_pandas()
        season_dfs.append(df)
        season_files_found = True

if season_files_found and season_dfs:
    atbat_df = pd.concat(season_dfs, ignore_index=True)
    # Filter by exact date range
    atbat_df = atbat_df[(pd.to_datetime(atbat_df["game_date"]) >= start_date) & (pd.to_datetime(atbat_df["game_date"]) <= end_date)]
else:
    st.error("No per-season index files found for the selected date range.")
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
        # Ensure sequence is a tuple of tuples to match the index
        sequence = tuple((p, o) for p, o in zip(pitch_inputs, outcome_inputs))
        hash_input = str(sequence).encode("utf-8")
        sequence_hash = hashlib.sha1(hash_input).hexdigest()


        matches = atbat_df[atbat_df["pitch_sequence_hash"] == sequence_hash]

        # Ensure matches is a DataFrame
        if isinstance(matches, pd.DataFrame) and not matches.empty:
            # 1. Gather all unique IDs as strings
            all_ids = pd.unique(matches[["pitcher", "batter"]].values.ravel())
            # all_ids = [int(x) for x in all_ids if x is not None]
        
            # 2. Lookup all IDs at once
            lookup_df = playerid_reverse_lookup(all_ids)
            lookup_df["full_name"] = lookup_df["name_first"] + " " + lookup_df["name_last"]
            lookup_df["key_mlbam"] = lookup_df["key_mlbam"].astype(str)  # Ensure string type

            # 3. Build lookup Series for fast mapping
            name_lookup = lookup_df.set_index("key_mlbam")["full_name"]


            # 4. Map names and images to matches DataFrame
            matches["pitcher_name"] = matches["pitcher"].astype(str).map(name_lookup)
            matches["batter_name"] = matches["batter"].astype(str).map(name_lookup)
            matches["pitcher_img"] = matches["pitcher"].astype(str).map(lambda x: f"https://securea.mlb.com/mlb/images/players/head_shot/{x}.jpg")
            matches["batter_img"] = matches["batter"].astype(str).map(lambda x: f"https://securea.mlb.com/mlb/images/players/head_shot/{x}.jpg")

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

            matches["statcast_url"] = matches.apply(build_statcast_url, axis=1)


            for _, row in matches.iterrows():
                st.markdown(
                    f"<div style='text-align: center;'>"
                    f"<h3>{row['pitcher_name'].title()} vs {row['batter_name'].title()} — {row['game_date']:%B %d, %Y}</h3>"
                    f"</div>",
                    unsafe_allow_html=True
)
                cols = st.columns([1, 6, 1])
                with cols[0]:
                    st.image(row["pitcher_img"], width=75)
                with cols[1]:
                    pitch_cols = st.columns(len(row["pitch_sequence"]))
                    for i, pitch in enumerate(row["pitch_sequence"]):
                        with pitch_cols[i]:
                            pitch_level_data = row["pitch_level_data"][i]


                            st.markdown(f"<div style='text-align:center;'>"
                                        f"<strong>{pitch_level_data['pitch_type']}</strong><br>"
                                        f"{pitch_level_data['release_speed']} mph<br>"
                                        f"Zone {int(pitch_level_data.get('zone', '–'))}"
                                        f"</div>", unsafe_allow_html=True)
                with cols[2]:
                    st.image(row["batter_img"], width=75)

                st.markdown(f"<div style='text-align: center;'><a href='{row['statcast_url']}' target='_blank'>🔗 Watch on Statcast</a></div>", unsafe_allow_html=True)
                st.markdown("---")
        else:
            st.subheader("No matching at-bats found.")
