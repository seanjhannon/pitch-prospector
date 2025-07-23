"""
Data Manager for Pitch Prospector
Handles daily updates and user-requested data fetching from Statcast API
"""

import os
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from pybaseball import statcast
from pitch_prospector.db import get_connection, insert_atbats
from pitch_prospector.indexing.pitch_index import process_file
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataManager:
    """Manages data fetching and updates for the Pitch Prospector app"""
    
    def __init__(self):
        self.conn = None
    
    def get_connection(self):
        """Get database connection"""
        if not self.conn or self.conn.closed:
            self.conn = get_connection()
        return self.conn
    
    def get_latest_game_date(self):
        """Get the latest game date in the database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT MAX(game_date) FROM atbats_simple")
                    result = cur.fetchone()
                    if result and result[0]:
                        return result[0].date()
                    return datetime(2015, 1, 1).date()  # Default to start of Statcast era
        except Exception as e:
            logger.error(f"Error getting latest game date: {e}")
            return date(datetime.today().year, 3, 1)
    
    def fetch_daily_updates(self, days_back=1):
        """
        Fetch and insert data for the last N days
        This should be run daily to keep the database current
        """
        logger.info(f"Starting daily update for last {days_back} days")
        
        end_date = datetime.today().date()
        start_date = end_date - timedelta(days=days_back)
        
        return self.fetch_and_insert_range(start_date, end_date)
    
    def fetch_and_insert_range(self, start_date, end_date):
        """
        Fetch and insert data for a specific date range
        This is used for both daily updates and user-requested data
        """
        logger.info(f"Fetching data from {start_date} to {end_date}")
        
        try:
            # Fetch data from Statcast API
            df = statcast(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
            
            if df.empty:
                logger.info("No data found for the specified date range")
                return 0
            
            # Process the data to at-bat level
            atbat_rows = process_file(df)
            
            if not atbat_rows:
                logger.info("No at-bats found in the data")
                return 0
            
            logger.info(f"Processing {len(atbat_rows)} at-bats")
            
            # Insert into database
            inserted_count = self._insert_atbat_data(atbat_rows)
            
            logger.info(f"Successfully inserted {inserted_count} at-bats")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error fetching data for {start_date} to {end_date}: {e}")
            return 0
    
    def _insert_atbat_data(self, atbat_rows):
        """Insert atbat data into database"""
        try:
            atbat_data = []
            for row in atbat_rows:
                # Ensure batter and pitcher are integers
                batter_id = int(row["batter"]) if row["batter"] is not None else 0
                pitcher_id = int(row["pitcher"]) if row["pitcher"] is not None else 0
                
                atbat_data.append({
                    "game_pk": row["game_pk"],
                    "at_bat_number": row["at_bat_number"],
                    "game_date": str(row["game_date"].date()) if hasattr(row["game_date"], "date") else str(row["game_date"]),
                    "batter": batter_id,
                    "pitcher": pitcher_id,
                    "inning": row["inning"],
                    "pitch_sequence_hash": row["pitch_sequence_hash"],
                    "pitch_sequence": row["pitch_sequence"],
                    "pitch_level_data": row["pitch_level_data"]
                })
            
            # Insert atbats with pitch data included
            insert_atbats(atbat_data)
            
            logger.info(f"Inserted {len(atbat_data)} atbats with pitch data")
            return len(atbat_data)
            
        except Exception as e:
            logger.error(f"Error inserting atbat data: {e}")
            return 0
    
    def ensure_current_season_data(self):
        """
        Ensure we have current season data
        This is called when the app starts to make sure we have recent data
        """
        logger.info("Ensuring current season data is available")
        
        latest_date = self.get_latest_game_date()
        today = datetime.today().date()
        
        # If we're missing recent data, fetch it
        if latest_date < today - timedelta(days=7):  # If we're missing more than a week
            logger.info(f"Missing recent data. Latest: {latest_date}, Today: {today}")
            return self.fetch_and_insert_range(latest_date + timedelta(days=1), today)
        
        logger.info("Current season data is up to date")
        return 0
    
    def close(self):
        """Close database connection"""
        if self.conn and not self.conn.closed:
            self.conn.close()

# Global data manager instance
data_manager = DataManager()

def get_data_manager():
    """Get the global data manager instance"""
    return data_manager 