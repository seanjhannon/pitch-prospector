import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
from pybaseball import playerid_reverse_lookup
import warnings
from dotenv import load_dotenv
import os
import time
from pitch_prospector.error_handling import (
    handle_database_errors, handle_player_lookup_errors, 
    validate_date_range, validate_pitch_sequence,
    safe_int_conversion, safe_str_conversion, log_and_display_error
)

# Load environment variables
load_dotenv()

from st_supabase_connection import SupabaseConnection

# Initialize connection.
conn = st.connection("supabase",type=SupabaseConnection)

warnings.filterwarnings("ignore", category=FutureWarning)

# --- Helper functions using official Supabase connection ---
def get_atbats_by_date_and_sequence_official(start_date, end_date, pitch_sequence):
    """
    Fetch atbats with specific date range and pitch sequence using official connection.
    pitch_sequence should be a list of [pitch_type, outcome] lists (matches database format).
    """
    try:
        print(f"🔍 Searching for sequence: {pitch_sequence}")
        
        # First, get at-bats in the date range with a reasonable limit
        # Start with a smaller limit to avoid overwhelming the connection
        limit = 10000
        result = conn.table("atbats_optimized").select(
            "id, game_pk, at_bat_number, game_date, batter, pitcher, inning, pitch_sequence"
        ).gte("game_date", start_date).lte("game_date", end_date).limit(limit).execute()
        
        if not result.data:
            print(f"  ❌ No data found in date range {start_date} to {end_date}")
            return []
        
        print(f"  📊 Retrieved {len(result.data)} at-bats (limited to {limit})")
        
        # Filter in Python to match the exact sequence
        matching_atbats = []
        for atbat in result.data:
            stored_sequence = atbat.get('pitch_sequence', [])
            if stored_sequence == pitch_sequence:
                matching_atbats.append(atbat)
        
        print(f"  ✅ Found {len(matching_atbats)} exact matches")
        
        # If we found matches, return them
        if matching_atbats:
            return matching_atbats
        
        # If no matches found, let's debug by showing some sample sequences
        print(f"  🔍 Debug: No exact matches found. Sample sequences from database:")
        for i, atbat in enumerate(result.data[:3]):
            sequence = atbat.get('pitch_sequence', [])
            print(f"    {i+1}. {sequence}")
        
        # Also check if we have any sequences that start with the same pitch
        first_pitch = pitch_sequence[0] if pitch_sequence else None
        if first_pitch:
            similar_sequences = []
            for atbat in result.data:
                stored_sequence = atbat.get('pitch_sequence', [])
                if stored_sequence and len(stored_sequence) > 0 and stored_sequence[0] == first_pitch:
                    similar_sequences.append(stored_sequence)
            
            if similar_sequences:
                print(f"  🔍 Found {len(similar_sequences)} sequences starting with {first_pitch}")
                print(f"  🔍 Sample similar sequences:")
                for i, seq in enumerate(similar_sequences[:3]):
                    print(f"    {i+1}. {seq}")
        
        return matching_atbats
        
    except Exception as e:
        print(f"❌ Error querying atbats: {e}")
        return []

def get_pitch_sequences_for_atbat_official(atbat_id):
    """
    Fetch pitch sequence data for a specific at-bat using official connection.
    """
    try:
        # Use the correct API for st-supabase-connection
        result = conn.table("atbats_optimized").select(
            "pitch_sequence, pitch_data"
        ).eq("id", atbat_id).execute()
        
        if result.data and len(result.data) > 0:
            row = result.data[0]
            pitch_sequence = row.get('pitch_sequence', [])
            pitch_data = row.get('pitch_data', [])
            
            # Combine pitch sequence with pitch data
            combined_data = []
            for i, pitch_tuple in enumerate(pitch_sequence):
                # Get corresponding pitch data (speed and zone)
                speed, zone = pitch_data[i] if i < len(pitch_data) else [0, 0]
                
                # Extract pitch_type and description from tuple
                if isinstance(pitch_tuple, (list, tuple)) and len(pitch_tuple) >= 2:
                    pitch_type, description = pitch_tuple[0], pitch_tuple[1]
                else:
                    pitch_type, description = str(pitch_tuple), 'unknown'
                
                combined_data.append({
                    'pitch_type': pitch_type,
                    'description': description,
                    'release_speed': speed,
                    'zone': zone
                })
            return combined_data
        return []
    except Exception as e:
        print(f"❌ Error querying pitch sequences: {e}")
        return []

# --- Helper: Check if DB is reachable and has data ---
@handle_database_errors
def db_is_available():
    print("🔍 Checking if database is available...")
    start_time = time.time()
    
    try:
        # Use the official Supabase connection to check if database has data
        result = conn.table("atbats_optimized").select("id", count="exact").limit(1).execute()
        
        if result.count is not None:
            count = result.count
            print(f"  📊 Found {count} records in database")
            has_data = count > 0
            print(f"  ✅ Database availability check completed in {time.time() - start_time:.2f}s")
            return has_data, count
        else:
            print("  ❌ Database query returned no results")
            return False, 0
    except Exception as e:
        print(f"  ❌ Database connection failed: {e}")
        return False, 0

# --- Helper: Get current MLB season range ---
def get_current_season_range():
    today = datetime.today()
    start = datetime(today.year, 3, 1)  # MLB season typically starts in March
    end = today
    return start, end

print("🚀 Starting app initialization...")
app_start_time = time.time()

db_available, atbat_count = db_is_available()
if db_available:
    print("📊 Database has data, ready to use")
else:
    print("❌ Database is not available")
    st.error("Database is not available. Please check your connection settings.")
    st.stop()

print(f"🎉 App initialization completed in {time.time() - app_start_time:.2f}s")

# --- UI ---
st.title("At-Bat Sequence Finder")
st.markdown(f"Pick a date range to search from {atbat_count:,} historical at-bats.")

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

# Validate date range
if not validate_date_range(start_date, end_date):
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
    # Validate pitch sequence
    if not validate_pitch_sequence(pitch_inputs, outcome_inputs):
        st.stop()
    
    print(f"🔍 User submitted search for {num_pitches} pitches")
    search_start_time = time.time()
    
    with st.spinner("Searching for matching at-bats..."):
        try:
            # Create sequence of lists (pitch_type, outcome) - matches database format
            sequence = [[p, o] for p, o in zip(pitch_inputs, outcome_inputs)]

            print(f"🔍 Querying atbats for range {start_date} to {end_date} with sequence...")
            query_start_time = time.time()
            
            # Use the optimized query that filters at database level
            matches = get_atbats_by_date_and_sequence_official(str(start_date), str(end_date), sequence)
            
            print(f"✅ Query completed in {time.time() - query_start_time:.2f}s, found {len(matches)} at-bats")

            if matches:
                all_ids = set()
                for row in matches:
                    # Player IDs are now integers in Supabase
                    pitcher_id = safe_int_conversion(row["pitcher"])
                    batter_id = safe_int_conversion(row["batter"])
                    all_ids.add(pitcher_id)
                    all_ids.add(batter_id)
                
                # Debug: see what IDs we're working with
                
                # Simplified player lookup without decorator
                name_lookup = {}
                try:
                    lookup_df = playerid_reverse_lookup(list(all_ids))
                    if not lookup_df.empty:
                        lookup_df["full_name"] = lookup_df["name_first"] + " " + lookup_df["name_last"]
                        lookup_df["key_mlbam"] = lookup_df["key_mlbam"].astype(str)
                        name_lookup = lookup_df.set_index("key_mlbam")["full_name"].to_dict()
                    else:
                        st.warning("No player names found, using player IDs instead")
                except Exception as e:
                    st.warning(f"Could not load player names: {e}")
                    name_lookup = {}

                for row in matches:
                    # Player IDs are now integers in Supabase
                    pitcher_id = safe_int_conversion(row["pitcher"])
                    batter_id = safe_int_conversion(row["batter"])
                    
                    pitcher_id_str = safe_str_conversion(pitcher_id)
                    batter_id_str = safe_str_conversion(batter_id)
                    
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

                    pitch_level_data = get_pitch_sequences_for_atbat_official(row["id"])

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
                
        except Exception as e:
            log_and_display_error(e, "Search error")
    
    print(f"🎉 Search completed in {time.time() - search_start_time:.2f}s")

