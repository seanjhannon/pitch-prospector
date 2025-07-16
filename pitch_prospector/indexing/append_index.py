# append_index.py

import os
import pandas as pd
from pathlib import Path
from indexing.pitch_index import process_file
from datetime import datetime
from tqdm import tqdm

DATA_DIR = "data/statcast_monthly"
INDEX_PATH = "data/atbat_pitch_sequence_index.parquet"

def get_latest_index_month(index_path):
    if not os.path.exists(index_path):
        return None
    df = pd.read_parquet(index_path, columns=["game_date"])
    last_date = pd.to_datetime(df["game_date"]).max()
    return last_date.strftime("%Y-%m")

def append_index_by_month():
    last_indexed_month = get_latest_index_month(INDEX_PATH)
    if not last_indexed_month:
        print("📂 No existing index found. Please run the full build instead.")
        return
    print(f"📅 Most recent indexed month: {last_indexed_month}")

    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith(".parquet")], reverse=True)
    new_rows = []

    for fname in tqdm(files):
        file_month = fname.replace(".parquet", "")
        if file_month <= last_indexed_month:
            break  # stop as soon as we hit an older or equal month

        fpath = os.path.join(DATA_DIR, fname)
        try:
            df = pd.read_parquet(fpath)
            df["game_date"] = pd.to_datetime(df["game_date"])
            recent_df = df[df["game_date"] > pd.to_datetime(last_indexed_month)]

            if not recent_df.empty:
                # Save to temp file and reuse existing logic
                temp_path = fpath.replace(".parquet", "_temp_filtered.parquet")
                recent_df.to_parquet(temp_path, index=False)
                new_rows.extend(process_file(temp_path))
                os.remove(temp_path)

        except Exception as e:
            print(f"❌ Failed to process {fname}: {e}")

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        # Only write/append to per-season index files
        if "game_year" not in new_df.columns:
            new_df["game_year"] = pd.to_datetime(new_df["game_date"]).dt.year
        for year, group in new_df.groupby("game_year"):
            season_path = f"data/atbat_pitch_sequence_index_{year}.parquet"
            if os.path.exists(season_path):
                season_df = pd.read_parquet(season_path)
                season_combined = pd.concat([season_df, group], ignore_index=True)
                # Deduplicate
                season_combined.drop_duplicates(subset=["game_pk", "at_bat_number"], keep="last", inplace=True)
            else:
                season_combined = group
            season_combined.to_parquet(season_path, index=False)
            print(f"✅ Appended {len(group):,} at-bats to {season_path}")
    else:
        print("✅ No new games found to index.")

if __name__ == "__main__":
    append_index_by_month()
