# Pitch Prospector

## Database Setup

The SQLite database file (`pitch_prospector/data/pitchprospector.sqlite`) is **not tracked in git** due to its size. To build or refresh the database:

### 1. Initial Migration (from Parquet, if needed)
If you have Parquet index files, run:
```bash
python scripts/init_db.py
python scripts/migrate_parquet_to_sqlite.py
```

### 2. Ongoing Updates (Statcast API)
Use the in-app refresh button or run the refresh logic:
```python
from pitch_prospector.indexing.cloud_refresh import refresh_sqlite_db
refresh_sqlite_db()
```

This will fetch new data from Statcast and index it into your SQLite DB.

## Note
- The DB file is ignored by git (`.gitignore`).
- Each developer or deployment should build/populate their own DB as needed.
