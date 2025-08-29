import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
from pybaseball import playerid_reverse_lookup, statcast
import warnings
from dotenv import load_dotenv
import os
import time
import psycopg
from pitch_prospector.error_handling import (
    handle_database_errors, handle_player_lookup_errors, 
    validate_date_range, validate_pitch_sequence,
    safe_int_conversion, safe_str_conversion, log_and_display_error
)

# Load environment variables
load_dotenv()

# --- Background refresh system ---
def auto_refresh_data():
    """Automatically refresh data daily using Streamlit caching"""
    try:
        # Get the most recent date in the database
        most_recent_date = get_most_recent_date()
        if not most_recent_date:
            return False, "Could not determine most recent date"
        
        today = datetime.today().date()
        
        # Only refresh if we're more than 1 day behind
        if most_recent_date.date() >= today - timedelta(days=1):
            return True, f"Database is up to date (most recent: {most_recent_date.date()})"
        
        # Calculate the date range to fetch
        start_date = most_recent_date + timedelta(days=1)
        end_date = today
        
        # Fetch Statcast data
        df = statcast(start_dt=str(start_date.date()), end_dt=str(end_date))
        
        if df.empty:
            return True, f"No new data available for {start_date.date()} to {end_date}"
        
        # Process data
        atbats = process_statcast_data_for_refresh(df)
        
        if not atbats:
            return True, "No valid at-bats found in new data"
        
        # Insert data with duplicate prevention
        inserted_count = insert_atbats_with_duplicate_prevention(atbats)
        
        if inserted_count > 0:
            return True, f"Successfully refreshed data: {inserted_count:,} new at-bats added"
        else:
            return True, "No new at-bats were added (all were duplicates)"
            
    except Exception as e:
        return False, f"Error refreshing data: {str(e)}"

# Cache the daily refresh function to run daily
@st.cache_data(ttl=86400)  # Cache for 24 hours (86400 seconds)
def run_daily_auto_refresh():
    """Run daily refresh once per day using Streamlit caching"""
    return auto_refresh_data()

def cleanup_duplicate_atbats():
    """Clean up existing duplicate at-bats by keeping only the most recent record for each unique at-bat."""
    try:
        print("🧹 Starting duplicate cleanup...")
        
        with get_cockroach_connection() as conn:
            with conn.cursor() as cur:
                # Find all duplicate at-bats based on natural key
                cur.execute("""
                    WITH duplicates AS (
                        SELECT 
                            game_pk, 
                            at_bat_number, 
                            game_date,
                            COUNT(*) as count,
                            MAX(id) as keep_id
                        FROM atbats_optimized 
                        GROUP BY game_pk, at_bat_number, game_date
                        HAVING COUNT(*) > 1
                    )
                    SELECT 
                        d.game_pk, 
                        d.at_bat_number, 
                        d.game_date, 
                        d.count,
                        d.keep_id,
                        array_agg(a.id) as all_ids
                    FROM duplicates d
                    JOIN atbats_optimized a ON 
                        d.game_pk = a.game_pk AND 
                        d.at_bat_number = a.at_bat_number AND 
                        d.game_date = a.game_date
                    GROUP BY d.game_pk, d.at_bat_number, d.game_date, d.count, d.keep_id
                    ORDER BY d.game_date DESC, d.game_pk, d.at_bat_number
                """)
                
                duplicates = cur.fetchall()
                
                if not duplicates:
                    print("✅ No duplicates found to clean up")
                    return 0
                
                print(f"🧹 Found {len(duplicates)} duplicate at-bat groups to clean up")
                
                total_deleted = 0
                for dup in duplicates:
                    game_pk, at_bat_number, game_date, count, keep_id, all_ids = dup
                    
                    # Delete all records except the one with the highest ID (most recent)
                    ids_to_delete = [str(id) for id in all_ids if id != keep_id]
                    
                    if ids_to_delete:
                        delete_query = f"""
                            DELETE FROM atbats_optimized 
                            WHERE id IN ({','.join(ids_to_delete)})
                        """
                        cur.execute(delete_query)
                        deleted_count = cur.rowcount
                        total_deleted += deleted_count
                        
                        print(f"   🗑️ Game {game_pk}, AB {at_bat_number}, {game_date}: Deleted {deleted_count} duplicates, kept ID {keep_id}")
                
                conn.commit()
                print(f"✅ Cleanup complete! Deleted {total_deleted} duplicate records")
                return total_deleted
                
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        import traceback
        traceback.print_exc()
        return 0

def debug_duplicate_atbats(pitch_sequence):
    """Debug function to identify duplicate at-bats for a specific pitch sequence."""
    try:
        import json
        sequence_json = json.dumps(pitch_sequence)
        
        with get_cockroach_connection() as conn:
            with conn.cursor() as cur:
                # Find all at-bats with this sequence
                cur.execute("""
                    SELECT id, game_pk, at_bat_number, game_date, batter, pitcher, inning, pitch_sequence
                    FROM atbats_optimized 
                    WHERE pitch_sequence = %s::jsonb
                    ORDER BY game_date DESC, game_pk, at_bat_number
                """, (sequence_json,))
                
                results = cur.fetchall()
                
                if not results:
                    print(f"🔍 No at-bats found for sequence: {pitch_sequence}")
                    return
                
                print(f"🔍 Found {len(results)} at-bats for sequence: {pitch_sequence}")
                
                # Check for duplicates based on natural key
                seen_keys = set()
                duplicates = []
                
                for row in results:
                    key = (row[1], row[2], row[3])  # (game_pk, at_bat_number, game_date)
                    if key in seen_keys:
                        duplicates.append(row)
                    seen_keys.add(key)
                
                if duplicates:
                    print(f"⚠️ Found {len(duplicates)} duplicate at-bats:")
                    for dup in duplicates:
                        print(f"   ID: {dup[0]}, Game: {dup[1]}, AB: {dup[2]}, Date: {dup[3]}")
                    
                    # Show the first few results to see the pattern
                    print(f"\n📊 First 10 results:")
                    for i, row in enumerate(results[:10]):
                        print(f"   {i+1}. ID: {row[0]}, Game: {row[1]}, AB: {row[2]}, Date: {row[3]}")
                else:
                    print("✅ No duplicates found in results")
                    
    except Exception as e:
        print(f"❌ Error debugging duplicates: {e}")
        import traceback
        traceback.print_exc()

# Background worker for continuous updates
def background_refresh_worker():
    """Background worker that refreshes data periodically while app is running"""
    # Initial delay to let app start up completely
    time.sleep(30)  # Wait 30 seconds after app starts
    
    while True:
        try:
            # Only run if we haven't refreshed recently
            most_recent = get_most_recent_date()
            if most_recent:
                hours_since_last = (datetime.now() - most_recent).total_seconds() / 3600
                if hours_since_last > 6:  # Only refresh if more than 6 hours old
                    # Silent refresh - no user notification
                    success, message = auto_refresh_data()
                # No logging for skipped refreshes
            
            # Wait 6 hours before next check
            time.sleep(21600)  # 6 hours = 21600 seconds
            
        except Exception as e:
            # Only log errors for debugging, don't show to users
            print(f"Background refresh error: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(3600)  # Wait 1 hour before retrying on error

# Initialize CockroachDB connection
def get_cockroach_connection():
    """Get a connection to CockroachDB."""
    try:
        # Try Streamlit secrets first (cloud)
        host = st.secrets["cockroachdb"]["host"]
        port = st.secrets["cockroachdb"]["port"]
        database = st.secrets["cockroachdb"]["database"]
        user = st.secrets["cockroachdb"]["user"]
        password = st.secrets["cockroachdb"]["password"]
        
        # Check if we need cluster identifier format (can be configured in secrets)
        cluster_id = st.secrets["cockroachdb"].get("cluster_id", "")
    except KeyError:
        # Fall back to environment variables (local)
        host = os.getenv("COCKROACH_HOST")
        port = os.getenv("COCKROACH_PORT")
        database = os.getenv("COCKROACH_DATABASE")
        user = os.getenv("COCKROACH_USER")
        password = os.getenv("COCKROACH_PASSWORD")
        cluster_id = os.getenv("COCKROACH_CLUSTER_ID", "")
    
    # Use cluster identifier if specified, otherwise standard format
    if cluster_id:
        dsn = f'postgresql://{user}:{password}@{host}:{port}/{cluster_id}.{database}?sslmode=require'
    else:
        dsn = f'postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=require'
    
    return psycopg.connect(dsn)

warnings.filterwarnings("ignore", category=FutureWarning)

# --- Helper functions using CockroachDB connection ---
def get_atbats_by_date_and_sequence_official(start_date, end_date, pitch_sequence):
    """
    Fetch atbats with specific date range and pitch sequence using CockroachDB connection.
    pitch_sequence should be a list of [pitch_type, outcome] lists (matches database format).
    """
    try:
        print(f"🔍 Searching for sequence: {pitch_sequence}")
        
        with get_cockroach_connection() as conn:
            with conn.cursor() as cur:
                # Use database-level filtering for exact sequence match
                # This is much more efficient than fetching and filtering in Python
                # Convert the sequence to proper JSON format for the query
                import json
                sequence_json = json.dumps(pitch_sequence)
                
                cur.execute("""
                    SELECT DISTINCT id, game_pk, at_bat_number, game_date, batter, pitcher, inning, pitch_sequence
                    FROM atbats_optimized 
                    WHERE game_date >= %s 
                    AND game_date <= %s
                    AND pitch_sequence = %s::jsonb
                    ORDER BY game_date DESC
                    LIMIT 1000
                """, (start_date, end_date, sequence_json))
                
                result_data = cur.fetchall()
        
        if not result_data:
            print(f"  ❌ No exact sequence matches found in date range {start_date} to {end_date}")
            
            # Debug: Check if the sequence exists at all in the database
            with get_cockroach_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM atbats_optimized 
                        WHERE pitch_sequence = %s::jsonb
                    """, (sequence_json,))
                    
                    total_matches = cur.fetchone()[0]
                    print(f"  🔍 Debug: Found {total_matches} total matches for this sequence across all dates")
                    
                    if total_matches > 0:
                        # Check what dates these matches occur in
                        cur.execute("""
                            SELECT MIN(game_date), MAX(game_date), COUNT(*)
                            FROM atbats_optimized 
                            WHERE pitch_sequence = %s
                            GROUP BY DATE_TRUNC('year', game_date)
                            ORDER BY DATE_TRUNC('year', game_date) DESC
                            LIMIT 5
                        """)
                        
                        year_counts = cur.fetchall()
                        print(f"  🔍 Debug: Sequence found in these years:")
                        for min_date, max_date, count in year_counts:
                            print(f"    {min_date.year}: {count:,} matches")
            
            return []
        
        print(f"  ✅ Found {len(result_data)} exact sequence matches")
        
        # Convert to the expected format
        atbats = []
        for row in result_data:
            atbat = {
                'id': row[0],
                'game_pk': row[1],
                'at_bat_number': row[2],
                'game_date': row[3],
                'batter': row[4],
                'pitcher': row[5],
                'inning': row[6],
                'pitch_sequence': row[7]
            }
            atbats.append(atbat)
        
        return atbats
        
    except Exception as e:
        print(f"❌ Error querying atbats: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_pitch_sequences_for_atbat_official(atbat_id):
    """
    Fetch pitch sequence data for a specific at-bat using CockroachDB connection.
    """
    try:
        with get_cockroach_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT pitch_sequence, pitch_data
                    FROM atbats_optimized 
                    WHERE id = %s
                """, (atbat_id,))
                
                result = cur.fetchone()
        
        if result:
            pitch_sequence = result[0]
            pitch_data = result[1]
            
            # Combine pitch sequence with pitch data
            combined_data = []
            for i, pitch_tuple in enumerate(pitch_sequence):
                # Get corresponding pitch data (speed and zone)
                speed, zone = pitch_data[i] if i < len(pitch_data) else ['0', '0']
                
                # Extract pitch_type and description from tuple
                if isinstance(pitch_tuple, (list, tuple)) and len(pitch_tuple) >= 2:
                    pitch_type, description = pitch_tuple[0], pitch_tuple[1]
                else:
                    pitch_type, description = str(pitch_tuple), 'unknown'
                
                # Convert string values back to appropriate types for display
                try:
                    release_speed = float(speed) if speed != '0' else 0
                    zone_num = int(zone) if zone != '0' else 0
                except (ValueError, TypeError):
                    release_speed = 0
                    zone_num = 0
                
                combined_data.append({
                    'pitch_type': pitch_type,
                    'description': description,
                    'release_speed': release_speed,
                    'zone': zone_num
                })
            return combined_data
        return []
    except Exception as e:
        print(f"❌ Error querying pitch sequences: {e}")
        return []

def get_most_recent_date():
    """Get the most recent date in the database."""
    try:
        with get_cockroach_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT game_date 
                    FROM atbats_optimized 
                    ORDER BY game_date DESC 
                    LIMIT 1
                """)
                result = cur.fetchone()
                
        if result:
            most_recent_date = pd.to_datetime(result[0])
            return most_recent_date
        return None
    except Exception as e:
        print(f"❌ Error getting most recent date: {e}")
        return None

def process_statcast_data_for_refresh(df):
    """Process Statcast data into optimized format for refresh."""
    if df.empty:
        return []
    
    atbats = []
    
    # Group by at-bat
    for (game_pk, at_bat_number), atbat_group in df.groupby(['game_pk', 'at_bat_number']):
        if atbat_group.empty:
            continue
            
        # Sort by pitch number to maintain sequence
        atbat_group = atbat_group.sort_values('pitch_number')
        
        # Extract pitch sequence and data
        pitch_sequence = []
        pitch_data = []
        
        for _, pitch in atbat_group.iterrows():
            pitch_type = str(pitch.get('pitch_type', 'UN'))
            description = str(pitch.get('description', 'unknown'))
            
            # Fill missing descriptions with reasonable defaults
            if description == 'nan' or description == 'unknown':
                if 'strike' in pitch_type.lower():
                    description = 'called_strike'
                elif 'ball' in pitch_type.lower():
                    description = 'ball'
                else:
                    description = 'hit_into_play'
            
            pitch_sequence.append([pitch_type, description])
            
            # Extract speed and zone - convert to strings for JSONB compatibility
            speed = float(pitch.get('release_speed', 0)) if pd.notna(pitch.get('release_speed')) else 0.0
            zone = int(pitch.get('zone', 0)) if pd.notna(pitch.get('zone')) else 0
            # Convert to strings to avoid mixed-type array issues in JSONB
            pitch_data.append([str(speed), str(zone)])
        
        # Create at-bat record
        first_pitch = atbat_group.iloc[0]
        atbat_record = {
            'game_pk': int(first_pitch['game_pk']),
            'at_bat_number': int(first_pitch['at_bat_number']),
            'game_date': str(first_pitch['game_date'].date()),
            'batter': int(first_pitch['batter']),
            'pitcher': int(first_pitch['pitcher']),
            'inning': int(first_pitch['inning']),
            'pitch_sequence': pitch_sequence,
            'pitch_data': pitch_data
        }
        
        atbats.append(atbat_record)
    
    return atbats

def insert_atbats_with_duplicate_prevention(atbats, batch_size=150):
    """Insert at-bats with duplicate prevention using upsert."""
    if not atbats:
        return 0
    
    total_inserted = 0
    total_batches = (len(atbats) + batch_size - 1) // batch_size
    
    # Progress bar for batch processing
    from tqdm import tqdm
    with tqdm(total=total_batches, desc="Inserting at-bats", unit="batch") as pbar:
        # Process in batches to avoid timeouts
        for i in range(0, len(atbats), batch_size):
            batch = atbats[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            try:
                # Use upsert to handle duplicates automatically
                with get_cockroach_connection() as conn:
                    with conn.cursor() as cur:
                        # Prepare the upsert query - use natural key for conflict resolution
                        upsert_query = """
                            INSERT INTO atbats_optimized (game_pk, at_bat_number, game_date, batter, pitcher, inning, pitch_sequence, pitch_data)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (game_pk, at_bat_number, game_date) DO UPDATE SET
                                batter = EXCLUDED.batter,
                                pitcher = EXCLUDED.pitcher,
                                inning = EXCLUDED.inning,
                                pitch_sequence = EXCLUDED.pitch_sequence,
                                pitch_data = EXCLUDED.pitch_data
                        """
                        
                        # Convert dictionary batch to tuple batch for executemany
                        batch_tuples = []
                        for atbat in batch:
                            # Convert Python lists to JSON strings for PostgreSQL JSONB
                            import json
                            pitch_sequence_json = json.dumps(atbat['pitch_sequence'])
                            pitch_data_json = json.dumps(atbat['pitch_data'])
                            
                            batch_tuples.append((
                                atbat['game_pk'],
                                atbat['at_bat_number'],
                                atbat['game_date'],
                                atbat['batter'],
                                atbat['pitcher'],
                                atbat['inning'],
                                pitch_sequence_json,  # JSON string
                                pitch_data_json       # JSON string
                            ))
                        
                        cur.executemany(upsert_query, batch_tuples)
                        conn.commit()
                        inserted_count = cur.rowcount
                        total_inserted += inserted_count
                        
                        # Update progress bar with batch info
                        pbar.set_postfix({
                            'Batch': f"{batch_num}/{total_batches}",
                            'Inserted': inserted_count,
                            'Total': total_inserted
                        })
                        pbar.update(1)
                
                # Small delay to avoid overwhelming the connection
                time.sleep(0.05)  # Reduced delay since we increased batch size
                
            except Exception as e:
                print(f"  ❌ Error inserting batch {batch_num}: {e}")
                pbar.update(1)  # Still update progress bar even on error
                continue
    
    return total_inserted

def refresh_recent_data():
    """Refresh data from most recent date to today."""
    try:
        # Get the most recent date in the database
        most_recent_date = get_most_recent_date()
        if not most_recent_date:
            return False, "Could not determine most recent date in database"
        
        today = datetime.today().date()
        
        # If we already have today's data, no need to refresh
        if most_recent_date.date() >= today:
            return True, f"Database is already up to date (most recent: {most_recent_date.date()})"
        
        # Calculate the date range to fetch
        start_date = most_recent_date + timedelta(days=1)  # Start from day after most recent
        end_date = today
        
        print(f"🔄 Refreshing data from {start_date.date()} to {end_date}")
        
        # Fetch Statcast data
        df = statcast(start_dt=str(start_date.date()), end_dt=str(end_date))
        
        if df.empty:
            return True, f"No new data available for {start_date.date()} to {end_date}"
        
        print(f"📊 Retrieved {len(df):,} pitches")
        
        # Process data
        atbats = process_statcast_data_for_refresh(df)
        
        if not atbats:
            return True, "No valid at-bats found in new data"
        
        print(f"🔄 Processing {len(atbats):,} at-bats")
        
        # Insert data with duplicate prevention
        inserted_count = insert_atbats_with_duplicate_prevention(atbats)
        
        if inserted_count > 0:
            return True, f"Successfully refreshed data: {inserted_count:,} new at-bats added"
        else:
            return True, "No new at-bats were added (all were duplicates)"
            
    except Exception as e:
        return False, f"Error refreshing data: {str(e)}"

# --- Helper: Check if DB is reachable and has data ---
@handle_database_errors
def db_is_available():
    print("🔍 Checking if database is available...")
    start_time = time.time()
    
    try:
        # Use the official Supabase connection to check if database has data
        with get_cockroach_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM atbats_optimized")
                count = cur.fetchone()[0]
                print(f"  📊 Found {count} records in database")
                has_data = count > 0
                print(f"  ✅ Database availability check completed in {time.time() - start_time:.2f}s")
                return has_data, count
    except Exception as e:
        print(f"  ❌ Database connection failed: {e}")
        return False, 0

# --- Helper: Get current MLB season range ---
def get_current_season_range():
    today = datetime.today()
    start = datetime(today.year, 3, 1)  # MLB season typically starts in March
    end = today
    return start, end

# Prevent duplicate initialization
if 'app_initialized' not in st.session_state:
    st.session_state.app_initialized = True
    
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
    
    # Start background refresh system (non-blocking)
    try:
        # Start background worker for continuous updates (non-blocking)
        import threading
        refresh_thread = threading.Thread(target=background_refresh_worker, daemon=True)
        refresh_thread.start()
        
        # Schedule daily refresh to run in background (non-blocking)
        def delayed_daily_refresh():
            time.sleep(5)  # Wait 5 seconds after app starts
            try:
                auto_refresh_success, auto_refresh_message = run_daily_auto_refresh()
            except Exception as e:
                print(f"Background refresh error: {e}")
        
        daily_refresh_thread = threading.Thread(target=delayed_daily_refresh, daemon=True)
        daily_refresh_thread.start()
        
        print("✅ Background refresh system started (non-blocking)")
        
    except Exception as e:
        # Only log errors, don't show to users
        print(f"Background refresh system error: {e}")
else:
    # App already initialized, just get the count
    db_available, atbat_count = db_is_available()



# --- UI ---
st.title("Pitch Prospector ⚾️⛏️")
st.markdown("*It's like Shazam for baseball!* ")
st.markdown(f"**How to use:** Pick a date range, then build a pitch sequence by selecting pitch types and outcomes. Search {atbat_count:,} historical at-bats for a matching sequence and watch replays on Savant!")




# Date range and refresh controls in one row
col1, col2, col3 = st.columns(3)
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
with col3:
    # Show most recent date and refresh button
    most_recent = get_most_recent_date()
    
    # Create help text that includes the latest date and last refresh info
    label_text = "🔄 Refresh Data"
    help_text = "Data typically available 12-24 hours after games"
    if most_recent:
        help_text += f" | Latest: {most_recent.date()}"
    
    # Add last manual refresh info to tooltip
    if 'last_manual_refresh' in st.session_state:
        help_text += f" | Last refresh: {st.session_state.last_manual_refresh}"
    else:
        help_text += " | No manual refresh yet"

    if st.button(label_text, type="secondary", key="refresh_button", 
    help=help_text):
        with st.spinner("Refreshing recent data..."):
            # Track manual refresh time
            st.session_state.last_manual_refresh = datetime.now().strftime("%Y-%m-%d %H:%M")
            
            success, message = refresh_recent_data()
            if success:
                st.success(message)
                # Refresh the page to show updated count
                st.rerun()
            else:
                st.error(message)
    



# Validate date range
if not validate_date_range(start_date, end_date):
    st.stop()


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

# Initialize session state for number of pitches
if 'num_pitches' not in st.session_state:
    st.session_state.num_pitches = 3

# Number of pitches selector (outside form for instant updates)
num_pitches = st.number_input(
    "Number of pitches in sequence", 
    min_value=1, 
    max_value=10, 
    value=st.session_state.num_pitches,
    key="num_pitches_input"
)

# Update session state when number changes
if num_pitches != st.session_state.num_pitches:
    st.session_state.num_pitches = num_pitches
    st.rerun()

# Pitch sequence form
with st.form("pitch_sequence_form"):
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
                # Display count of matches found
                st.success(f"🎯 Found {len(matches):,} matching at-bat{'s' if len(matches) != 1 else ''}!")
                
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

                    # Create a card-like container using Streamlit components
                    with st.container():
                        # Card header
                        st.markdown(f"### {row['pitcher_name'].title()} vs {row['batter_name'].title()} | {pd.to_datetime(row['game_date']):%B %d, %Y}")
                        
                        # Main content area with columns
                        col1, col2, col3 = st.columns([1, 3, 1])
                        
                        with col1:
                            try:
                                st.image(row["pitcher_img"], width=60)
                            except:
                                st.markdown("🫥")
                        
                        with col2:
                            # Display pitches in a horizontal layout
                            if len(pitch_level_data) <= 4:
                                # For 4 or fewer pitches, use columns
                                pitch_cols = st.columns(len(pitch_level_data))
                                for i, pitch in enumerate(pitch_level_data):
                                    with pitch_cols[i]:
                                        st.markdown(f"**{pitch['pitch_type']}**")
                                        st.markdown(f"{pitch['release_speed']} mph")
                                        st.markdown(f"Zone {int(pitch.get('zone', '–'))}")
                            else:
                                # For more pitches, stack them
                                for i, pitch in enumerate(pitch_level_data):
                                    with st.expander(f"Pitch {i+1}: {pitch['pitch_type']}", expanded=True):
                                        st.markdown(f"**Speed:** {pitch['release_speed']} mph")
                                        st.markdown(f"**Zone:** {int(pitch.get('zone', '–'))}")
                        
                        with col3:
                            try:
                                st.image(row["batter_img"], width=60)
                            except:
                                st.markdown("🫥")
                        
                        # Statcast link
                        st.markdown(f"[🔗 Watch on Savant]({row['statcast_url']})")
                        
                        # Divider
                        st.divider()
            else:
                st.subheader("No matching at-bats found.")
                
        except Exception as e:
            log_and_display_error(e, "Search error")
    
    print(f"🎉 Search completed in {time.time() - search_start_time:.2f}s")

