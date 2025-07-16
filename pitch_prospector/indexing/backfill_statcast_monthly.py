# backfill_statcast_by_month.py

# this pulls all the raw data from statcast to populate the data dir

import os
from datetime import datetime, timedelta
import pandas as pd
from pybaseball import statcast
from tqdm import tqdm
import warnings

# Ignore statcast pull error messages on pybaseball infra end
warnings.filterwarnings(
    "ignore",
    message=".*errors='ignore' is deprecated.*",
    category=FutureWarning,
    module="pybaseball.datahelpers.postprocessing"
)


DATA_DIR = "pitch_prospector/data/statcast_monthly"
START_YEAR = 2015
NOW = datetime.now()


os.makedirs(DATA_DIR, exist_ok=True)

all_months = []
for year in range(START_YEAR, NOW.year + 1):
    for month in range(1, 13):
        start = datetime(year, month, 1)
        if start > NOW:
            continue
        all_months.append(start)

print(f"📦 Beginning historical backfill: {len(all_months)} months to check")
for start in tqdm(all_months):
    end = (start + timedelta(days=32)).replace(day=1)
    fname = f"{start.year}-{start.month:02d}.parquet"
    fpath = os.path.join(DATA_DIR, fname)

    if os.path.exists(fpath):
        continue

    try:
        df = statcast(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        if df.empty:
            continue
        cols_in_df = [col for col in df.columns]
        df = df[cols_in_df]
        df.to_parquet(fpath, index=False)
    except Exception as e:
        print(f"❌ Failed to download {fname}: {e}")
