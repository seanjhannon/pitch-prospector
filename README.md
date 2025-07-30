# Pitch Prospector

A Streamlit web application for searching MLB at-bat sequences using pitch type and outcome combinations.

## Features

- **Pitch Sequence Search**: Find historical at-bats matching specific pitch type and outcome sequences
- **Date Range Filtering**: Search within custom date ranges from 2015 to present
- **Player Information**: View pitcher and batter details with MLB player images
- **Statcast Integration**: Direct links to Baseball Savant for detailed pitch analysis
- **Real-time Data**: Fetches data directly from MLB Statcast API via pybaseball
- **Data Refresh**: One-click button to update database with recent games

## Technology Stack

- **Frontend**: Streamlit
- **Database**: Supabase PostgreSQL
- **Data Source**: MLB Statcast API (via pybaseball)
- **Connection**: st-supabase-connection

## Setup

### 1. Environment Setup
```bash
# Install dependencies
poetry install

# Set up environment variables
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edit .streamlit/secrets.toml with your Supabase credentials
```

### 2. Database Setup
The application uses Supabase PostgreSQL. The database schema is automatically created when you run the data pipeline.

### 3. Data Population
To populate the database with historical data:
```bash
# Run the population script to reach target size (0.4GB)
poetry run python scripts/populate_to_target_size_v3.py
```

### 4. Run the Application
```bash
poetry run streamlit run pitch_prospector/app.py
```

## Database Schema

The application uses an optimized schema (`atbats_optimized`) that stores:
- **Essential fields**: game_pk, at_bat_number, game_date, batter, pitcher, inning
- **Pitch sequences**: JSONB array of [pitch_type, outcome] pairs
- **Pitch data**: JSONB array of [release_speed, zone] pairs

## Data Management

### Initial Population
Use `scripts/populate_to_target_size_v3.py` to populate the database with historical data from 2015 onwards.

### Keeping Data Fresh
The app includes a **"Refresh Recent Data"** button that:
- Automatically detects the most recent date in the database
- Fetches new data from that date to today
- Uses duplicate prevention to avoid re-inserting existing data
- Processes data in small batches to avoid timeouts
- Updates the database count automatically

### Manual Refresh Process
1. Click the "🔄 Refresh Recent Data" button in the app
2. The system fetches data from the most recent date + 1 day to today
3. New at-bats are processed and inserted with duplicate prevention
4. The page refreshes to show the updated record count

## Scripts

- `scripts/populate_to_target_size_v3.py` - Populate database with historical data
- `scripts/optimized_data_pipeline.py` - Data ingestion pipeline from Statcast API

## Notes

- The application fetches data directly from MLB Statcast API
- No local data files are required
- Database is hosted on Supabase cloud
- Optimized for minimal storage while maintaining search functionality
- Duplicate prevention ensures data integrity during refreshes
