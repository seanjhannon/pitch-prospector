# pitch_index.py

# Holds all the helpers and reused logic for build_index.py and append_index.py

import os
import pandas as pd
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed

COLUMNS_TO_KEEP = [
    "game_date", "game_year", "game_pk",
    "at_bat_number", "pitch_number",
    "batter", "pitcher",
    "pitch_type", "pitch_name", "description", "des", "events",
    "balls", "strikes", "inning", "inning_topbot",
    "release_speed", "plate_x", "plate_z", "zone",
    "home_team", "away_team", "stand", "p_throws", "outs_when_up",
    "release_spin_rate", "release_extension",
    "hit_distance_sc", "launch_speed", "launch_angle",
    "home_score", "away_score", "bat_score", "fld_score"
]

def process_file(data_or_path, existing_keys=None):
    try:
        if isinstance(data_or_path, pd.DataFrame):
            df = data_or_path
        else:
            df = pd.read_parquet(data_or_path)
        cols_available = [col for col in COLUMNS_TO_KEEP if col in df.columns]
        df = df[cols_available]
        df = df.sort_values(by=["game_pk", "at_bat_number", "pitch_number"])
        grouped = df.groupby(["game_pk", "at_bat_number"], sort=False)

        rows = []
        for (game_pk, ab_num), group in grouped:
            if existing_keys and (game_pk, ab_num) in existing_keys:
                continue

            pitch_data = group.to_dict(orient="records")
            # Ensure pitch_sequence is always a tuple of tuples
            pitch_sequence = tuple((p.get("pitch_type"), p.get("description")) for p in pitch_data)
            hash_input = str(pitch_sequence).encode("utf-8")
            pitch_sequence_hash = hashlib.sha1(hash_input).hexdigest()

            rows.append({
                "game_date": group.iloc[0]["game_date"],
                "game_pk": game_pk,
                "at_bat_number": ab_num,
                "batter": group.iloc[0]["batter"],
                "pitcher": group.iloc[0]["pitcher"],
                "inning": group.iloc[0]["inning"],
                "pitch_sequence": pitch_sequence,
                "pitch_sequence_hash": pitch_sequence_hash,
                "pitch_level_data": pitch_data
            })
        return rows
    except Exception as e:
        print(f"❌ Failed to load {type(data_or_path)}: {e}")
        return []

def process_all_files(data_dir, existing_keys=None, max_workers=4):
    files = [os.path.join(data_dir, f) for f in sorted(os.listdir(data_dir)) if f.endswith(".parquet")]
    all_rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_file, fpath, existing_keys) for fpath in files]
        for future in as_completed(futures):
            all_rows.extend(future.result())
    return all_rows

def load_existing_keys(index_path):
    if not os.path.exists(index_path):
        return set()
    df = pd.read_parquet(index_path, columns=["game_pk", "at_bat_number"])
    return set(zip(df["game_pk"], df["at_bat_number"]))
