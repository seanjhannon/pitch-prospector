import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pitch_prospector.db import get_atbats_by_date_range, get_pitch_sequences_for_atbat
from pitch_prospector.indexing.pitch_index import process_file, insert_new_data_from_indexed_rows
from pybaseball import playerid_reverse_lookup, statcast
import warnings
from dotenv import load_dotenv
import os
import time

# Load environment variables
load_dotenv()

warnings.filterwarnings("ignore", category=FutureWarning)

# --- Helper: Check if DB is reachable and has data ---
def db_is_available():
    print("🔍 Checking if database is available...")
    start_time = time.time()
    
    # Use a simple COUNT query to check if database is reachable and has data
    import psycopg2
    conn = psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=os.environ.get("SUPABASE_DB_PORT", 5432),
        dbname=os.environ["SUPABASE_DB_NAME"],
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"]
    )
    
    try:
        with conn.cursor() as cur:
            print("  📊 Executing COUNT query...")
            cur.execute("SELECT COUNT(*) FROM atbats_simple")
            result = cur.fetchone()
            if result is None:
                print("  ❌ Database query returned no results")
                return False, 0
            count = result[0]
            print(f"  📊 Found {count} records in database")
            
            has_data = count > 0
            print(f"  ✅ Database availability check completed in {time.time() - start_time:.2f}s")
            return has_data, count
    except Exception as e:
        print(f"  ❌ Database connection failed: {e}")
        return False, 0
    finally:
        conn.close()

# --- Helper: Get current MLB season range ---
def get_current_season_range():
    today = datetime.today()
    start = datetime(today.year, 3, 1)  # MLB season typically starts in March
    end = today
    return start, end

def fetch_process_month(year, month):
    print(f"  🔄 Fetching {year}-{month:02d}...")
    start_time = time.time()
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
        print(f"  ❌ Error fetching {year}-{month:02d}: {e}")
    if df.empty:
        print(f"  ⚠️  No data for {year}-{month:02d}")
        return []
    print(f"  📊 Processing {len(df)} records for {year}-{month:02d}...")
    atbat_rows = process_file(df)
    print(f"  ✅ {year}-{month:02d} completed in {time.time() - start_time:.2f}s ({len(atbat_rows)} at-bats)")
    return atbat_rows

# --- Populate current season on startup if needed ---
def populate_current_season():
    print("🚀 Starting current season population...")
    start_time = time.time()
    start, end = get_current_season_range()
    months = pd.date_range(start, end, freq='MS')
    print(f"📅 Processing {len(months)} months from {start.strftime('%Y-%m')} to {end.strftime('%Y-%m')}")
    
    all_atbat_rows = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_process_month, d.year, d.month) for d in months]
        for f in as_completed(futures):
            atbat_rows = f.result()
            if atbat_rows:
                all_atbat_rows.extend(atbat_rows)
    
    if all_atbat_rows:
        print(f"📥 Inserting {len(all_atbat_rows)} at-bats...")
        insert_start = time.time()
        insert_new_data_from_indexed_rows(all_atbat_rows)
        print(f"✅ Insertion completed in {time.time() - insert_start:.2f}s")
    
    print(f"🎉 Current season population completed in {time.time() - start_time:.2f}s")

print("🚀 Starting app initialization...")
app_start_time = time.time()

db_available, atbat_count = db_is_available()
if not db_available:
    print("📭 Database is empty or unreachable, loading current MLB season data...")
    with st.spinner("Loading current MLB season data (first use)..."):
        populate_current_season()
    st.success("Current season loaded! You can now use the app.")
else:
    print("📊 Database has data, ready to use")

print(f"🎉 App initialization completed in {time.time() - app_start_time:.2f}s")

# --- On-demand fetch for user-selected date range ---
def fetch_and_insert_for_range(start_date, end_date):
    print(f"🔄 Fetching data for range {start_date} to {end_date}...")
    start_time = time.time()
    months = pd.date_range(start_date, end_date, freq='MS')
    all_atbat_rows = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_process_month, d.year, d.month) for d in months]
        for f in as_completed(futures):
            atbat_rows = f.result()
            if atbat_rows:
                all_atbat_rows.extend(atbat_rows)
    if all_atbat_rows:
        print(f"📥 Inserting {len(all_atbat_rows)} at-bats...")
        insert_new_data_from_indexed_rows(all_atbat_rows)
    print(f"✅ Range fetch completed in {time.time() - start_time:.2f}s")

# --- UI ---
st.title("At-Bat Sequence Finder")
st.markdown("Pick a date range to filter historical at-bats.")

# Display fun factoid about database size
if atbat_count > 0:
    st.info(f"📊 **Fun fact:** This database contains {atbat_count:,} at-bats from 2015 to today!")

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
    return start_date < current_start.date() or end_date > current_end.date()

if range_needs_fetch(start_date, end_date):
    if st.button("Fetch Data for Selected Range"):
        with st.spinner("Fetching and processing data for selected range..."):
            fetch_and_insert_for_range(start_date, end_date)
        st.success("Data for selected range loaded!")

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
    print(f"🔍 User submitted search for {num_pitches} pitches")
    search_start_time = time.time()
    
    with st.spinner("Searching for matching at-bats..."):
        # Only query the database when user actually searches
        print(f"🔍 Querying at-bats for range {start_date} to {end_date}...")
        query_start_time = time.time()
        atbat_records = get_atbats_by_date_range(str(start_date), str(end_date))
        print(f"✅ Query completed in {time.time() - query_start_time:.2f}s, found {len(atbat_records)} at-bats")
        
        sequence = tuple((p, o) for p, o in zip(pitch_inputs, outcome_inputs))
        hash_input = str(sequence).encode("utf-8")
        sequence_hash = hashlib.sha1(hash_input).hexdigest()

        # Only consider at-bats in the selected date range
        matches = [row for row in atbat_records if row["pitch_sequence_hash"] == sequence_hash]

        if matches:
            all_ids = set()
            for row in matches:
                # Player IDs are now integers in Supabase
                pitcher_id = row["pitcher"]
                batter_id = row["batter"]
                all_ids.add(pitcher_id)
                all_ids.add(batter_id)
            
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
                # Player IDs are now integers in Supabase
                pitcher_id = row["pitcher"]
                batter_id = row["batter"]
                
                pitcher_id_str = str(pitcher_id)
                batter_id_str = str(batter_id)
                
                row["pitcher_name"] = name_lookup.get(pitcher_id_str, f"Player {pitcher_id}")
                row["batter_name"] = name_lookup.get(batter_id_str, f"Player {batter_id}")
                row["pitcher_img"] = f"https://securea.mlb.com/mlb/images/players/head_shot/{pitcher_id}.jpg"
                row["batter_img"] = f"https://securea.mlb.com/mlb/images/players/head_shot/{batter_id}.jpg"

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
    
    print(f"🎉 Search completed in {time.time() - search_start_time:.2f}s")
