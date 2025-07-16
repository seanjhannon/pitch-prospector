# build_index.py

import os
import pandas as pd
from pathlib import Path
from pitch_index import process_file
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


DATA_DIR = "pitch_prospector/data/statcast_monthly"
INDEX_PATH = "pitch_prospector/data/atbat_pitch_sequence_index.parquet"

def build_index():
    print(f"🔄 Rebuilding pitch index from all files in: {DATA_DIR}")
    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith(".parquet")])
    file_paths = [os.path.join(DATA_DIR, f) for f in files]

    all_rows = []

    print(f"📦 Found {len(file_paths)} files to process.")
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_file, fpath): fpath for fpath in file_paths}
        with tqdm(total=len(futures), desc="🔧 Processing files") as pbar:
            for future in as_completed(futures):
                result = future.result()
                all_rows.extend(result)
                pbar.update(1)

    if not all_rows:
        print("⚠️ No data found. Index not created.")
        return

    df = pd.DataFrame(all_rows)

    # Deduplicate in case of duplicate data files or overlapping at-bats
    df.drop_duplicates(subset=["game_pk", "at_bat_number"], keep="last", inplace=True)

    # Only write per-season index files
    if "game_year" not in df.columns:
        df["game_year"] = pd.to_datetime(df["game_date"]).dt.year
    for year, group in df.groupby("game_year"):
        season_path = f"pitch_prospector/data/atbat_pitch_sequence_index_{year}.parquet"
        group.to_parquet(season_path, index=False)
        print(f"✅ Season {year}: {len(group):,} at-bats saved to {season_path}")

    print(f"✅ Index built successfully: {len(df):,} unique at-bats saved to per-season files.")

if __name__ == "__main__":
    build_index()
