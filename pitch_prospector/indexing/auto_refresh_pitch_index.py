# auto_refresh_pitch_index.py

import os
from datetime import datetime, timedelta
import pandas as pd
from pybaseball import statcast
from tqdm import tqdm
import warnings
from pathlib import Path
import pyarrow.parquet as pq
from indexing.pitch_index import process_file

# -------- CONFIG -------- #
DATA_DIR = "data/statcast_monthly"
INDEX_PATH = "data/atbat_pitch_sequence_index.parquet"

# -------- WARNINGS -------- #
warnings.filterwarnings(
    "ignore",
    message=".*errors='ignore' is deprecated.*",
    category=FutureWarning,
    module="pybaseball.datahelpers.postprocessing"
)

# -------- HELPERS -------- #
def get_latest_index_date(index_path):
    if not os.path.exists(index_path):
        return None
    df = pd.read_parquet(index_path, columns=["game_date"])
    return pd.to_datetime(df["game_date"]).max()

def get_month_start_dates(start_date, end_date):
    months = []
    date = start_date.replace(day=1)
    while date <= end_date:
        months.append(date)
        date = (date + timedelta(days=32)).replace(day=1)
    return months

def download_statcast_month(start):
    end = (start + timedelta(days=32)).replace(day=1)
    fname = f"{start.year}-{start.month:02d}.parquet"
    fpath = os.path.join(DATA_DIR, fname)

    if os.path.exists(fpath):
        return fpath

    try:
        df = statcast(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        if df.empty:
            return None
        df.to_parquet(fpath, index=False)
        return fpath
    except Exception as e:
        print(f"❌ Failed to download {fname}: {e}")
        return None

def append_new_data(last_indexed_date):
    files = sorted([f for f in os.listdir(DATA_DIR) if f.endswith(".parquet")], reverse=True)
    new_rows = []

    for fname in files:
        file_month = fname.replace(".parquet", "")
        file_date = pd.to_datetime(file_month + "-01")
        if file_date < last_indexed_date.replace(day=1):
            break

        fpath = os.path.join(DATA_DIR, fname)
        try:
            df = pd.read_parquet(fpath)
            df["game_date"] = pd.to_datetime(df["game_date"])
            recent_df = df[df["game_date"] >= last_indexed_date]

            if not recent_df.empty:
                temp_path = fpath.replace(".parquet", "_temp_filtered.parquet")
                recent_df.to_parquet(temp_path, index=False)
                new_rows.extend(process_file(temp_path))
                os.remove(temp_path)

        except Exception as e:
            print(f"❌ Failed to process {fname}: {e}")

    if not new_rows:
        print("✅ No new at-bats found to index.")
        return

    new_df = pd.DataFrame(new_rows)
    # Only write/append to per-season index files
    if "game_year" not in new_df.columns:
        new_df["game_year"] = pd.to_datetime(new_df["game_date"]).dt.year
    for year, group in new_df.groupby("game_year"):
        season_path = f"data/atbat_pitch_sequence_index_{year}.parquet"
        if os.path.exists(season_path):
            season_df = pd.read_parquet(season_path)
            season_combined = pd.concat([season_df, group], ignore_index=True)
            season_combined.drop_duplicates(subset=["game_pk", "at_bat_number"], keep="last", inplace=True)
        else:
            season_combined = group
        season_combined.to_parquet(season_path, index=False)
        print(f"✅ Appended {len(group):,} at-bats to {season_path}")

# -------- MAIN REFRESH ENTRYPOINT -------- #
def auto_refresh_pitch_index():
    print("🔄 Starting automated pitch index refresh...")

    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

    last_indexed_date = get_latest_index_date(INDEX_PATH)
    if not last_indexed_date:
        print("❌ No existing pitch index found. Run full build instead.")
        return

    print(f"📅 Last indexed game date: {last_indexed_date.date()}")
    today = datetime.now()

    # STEP 1: Download all missing months
    months_to_check = get_month_start_dates(last_indexed_date, today)
    print(f"📦 Checking {len(months_to_check)} months of new data...")
    for start in tqdm(months_to_check):
        download_statcast_month(start)

    # STEP 2: Append new data
    append_new_data(last_indexed_date)
