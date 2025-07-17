import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pitch_prospector.db import get_atbats_by_date_range, get_atbats_by_sequence_hash, get_pitch_sequences_for_atbat
from pitch_prospector.indexing.pitch_index import process_file, insert_new_data_from_indexed_rows
from pitch_prospector.db import init_db_main
from pybaseball import playerid_reverse_lookup, statcast
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

DB_PATH = "pitch_prospector/data/pitchprospector.sqlite"

# --- DB Initialization ---
def ensure_db_initialized():
    with st.spinner("Ensuring database schema..."):
        init_db_main(DB_PATH)
ensure_db_initialized()

# --- Helper: Check if DB is empty ---
def db_is_empty():
    records = get_atbats_by_date_range("2015-01-01", str(datetime.today().date()))
    return not records

# --- Helper: Get current MLB season range ---
def get_current_season_range():
    today = datetime.today()
    start = datetime(today.year, 3, 1)  # MLB season typically starts in March
    end = today
    return start, end

def fetch_process_month(year, month):
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year + 1, 1, 1)
    else:
        end = datetime(year, month + 1, 1)
    df = pd.DataFrame()
    try:
        from pybaseball import statcast
        df = statcast(start.strftime("%Y-%m-%d"), (end - timedelta(days=1)).strftime("%Y-%m-%d"))
    except Exception as e:
        print(f"Error fetching {year}-{month:02d}: {e}")
    if df.empty:
        return []
    atbat_rows = process_file(df)
    return atbat_rows

# --- Populate current season on startup if needed ---
def populate_current_season():
    start, end = get_current_season_range()
    months = pd.date_range(start, end, freq='MS')
    all_atbat_rows = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_process_month, d.year, d.month) for d in months]
        for f in as_completed(futures):
            atbat_rows = f.result()
            if atbat_rows:
                all_atbat_rows.extend(atbat_rows)
    if all_atbat_rows:
        insert_new_data_from_indexed_rows(all_atbat_rows)

if db_is_empty():
    with st.spinner("Loading current MLB season data (first use)..."):
        populate_current_season()
    st.success("Current season loaded! You can now use the app.")

# --- On-demand fetch for user-selected date range ---
def fetch_and_insert_for_range(start_date, end_date):
    months = pd.date_range(start_date, end_date, freq='MS')
    all_atbat_rows = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_process_month, d.year, d.month) for d in months]
        for f in as_completed(futures):
            atbat_rows = f.result()
            if atbat_rows:
                all_atbat_rows.extend(atbat_rows)
    if all_atbat_rows:
        insert_new_data_from_indexed_rows(all_atbat_rows)

# --- UI ---
st.title("At-Bat Sequence Finder")
st.markdown("Pick a date range to filter historical at-bats.")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input(
        "Start date",
        value=get_current_season_range()[0],
        min_value=datetime(2015, 1, 1),
        max_value=datetime.today()
    )
with col2:
    end_date = st.date_input(
        "End date",
        value=datetime.today(),
        min_value=start_date,
        max_value=datetime.today()
    )

# If user selects a range outside the current season, fetch missing months
def range_needs_fetch(start_date, end_date):
    current_start, current_end = get_current_season_range()
    # st.write(type(current_start), type(current_end), type(start_date), type(end_date))
    return start_date < current_start.date() or end_date > current_end.date()

if range_needs_fetch(start_date, end_date):
    if st.button("Fetch Data for Selected Range"):
        with st.spinner("Fetching and processing data for selected range..."):
            fetch_and_insert_for_range(start_date, end_date)
        st.success("Data for selected range loaded!")

# --- Query and display at-bats as before ---
atbat_records = get_atbats_by_date_range(str(start_date), str(end_date))
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

        # Only consider at-bats in the selected date range
        matches = [row for row in atbat_records if row["pitch_sequence_hash"] == sequence_hash]

        if matches:
            all_ids = set()
            for row in matches:
                all_ids.add(str(row["pitcher"]))  # Convert to string
                all_ids.add(str(row["batter"]))   # Convert to string
            
            # Debug: see what IDs we're working with
            st.write("Debug - Player IDs:", list(all_ids)[:5])  # Show first 5 IDs
            
            try:
                lookup_df = playerid_reverse_lookup(list(all_ids))
                lookup_df["full_name"] = lookup_df["name_first"] + " " + lookup_df["name_last"]
                lookup_df["key_mlbam"] = lookup_df["key_mlbam"].astype(str)
                name_lookup = lookup_df.set_index("key_mlbam")["full_name"]
                st.write("Debug - Lookup successful, found names for:", len(lookup_df), "players")
            except Exception as e:
                st.warning(f"Could not load player names: {e}")
                name_lookup = {}

            for row in matches:
                pitcher_id = str(row["pitcher"])
                batter_id = str(row["batter"])
                
                # Clean up the ID display - if it looks like bytes, extract the number
                def clean_id(id_str):
                    if id_str.startswith("B'") and id_str.endswith("'"):
                        # Extract the numeric part from bytes representation
                        try:
                            # Convert bytes back to integer
                            import ast
                            bytes_obj = ast.literal_eval(id_str)
                            if isinstance(bytes_obj, bytes):
                                # Convert bytes to integer (little endian)
                                return str(int.from_bytes(bytes_obj, byteorder='little'))
                        except:
                            pass
                    return id_str
                
                clean_pitcher_id = clean_id(pitcher_id)
                clean_batter_id = clean_id(batter_id)
                
                row["pitcher_name"] = name_lookup.get(clean_pitcher_id, f"Player {clean_pitcher_id}")
                row["batter_name"] = name_lookup.get(clean_batter_id, f"Player {clean_batter_id}")
                row["pitcher_img"] = f"https://securea.mlb.com/mlb/images/players/head_shot/{clean_pitcher_id}.jpg"
                row["batter_img"] = f"https://securea.mlb.com/mlb/images/players/head_shot/{clean_batter_id}.jpg"

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
