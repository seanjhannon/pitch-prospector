#!/usr/bin/env python3
"""
Script to recreate the optimized table with pitch outcomes included using Streamlit Supabase connection.
This updates the schema to store pitch sequences as (pitch_type, outcome) tuples.
"""

import os
from dotenv import load_dotenv
import streamlit as st
from st_supabase_connection import SupabaseConnection

load_dotenv()

# Initialize Supabase connection
conn = st.connection("supabase", type=SupabaseConnection)

def recreate_optimized_table():
    """Recreate the optimized table with the new schema including outcomes."""
    print("🔧 Recreating optimized table with outcomes...")
    
    try:
        # Note: DDL operations require admin privileges in Supabase
        # For now, we'll provide the SQL to run manually
        print("⚠️ Table recreation requires admin privileges. Please run this SQL manually:")
        print("""
        DROP TABLE IF EXISTS atbats_optimized;
        
        CREATE TABLE atbats_optimized (
            id SERIAL PRIMARY KEY,
            game_pk BIGINT NOT NULL,
            at_bat_number INTEGER NOT NULL,
            game_date DATE NOT NULL,
            batter BIGINT NOT NULL,
            pitcher BIGINT NOT NULL,
            inning INTEGER NOT NULL,
            pitch_sequence JSONB NOT NULL,  -- Now stores [pitch_type, outcome] tuples
            pitch_data JSONB NOT NULL,      -- Still stores [release_speed, zone] pairs
            UNIQUE(game_pk, at_bat_number)
        );
        
        CREATE INDEX idx_opt_game_date ON atbats_optimized(game_date);
        CREATE INDEX idx_opt_pitch_sequence ON atbats_optimized USING GIN(pitch_sequence);
        CREATE INDEX idx_opt_date_sequence ON atbats_optimized(game_date, pitch_sequence);
        CREATE INDEX idx_opt_pitcher ON atbats_optimized(pitcher);
        CREATE INDEX idx_opt_batter ON atbats_optimized(batter);
        """)
        
        print("✅ Table recreation instructions provided")
        print("📊 New schema stores pitch_sequence as [pitch_type, outcome] tuples")
        
    except Exception as e:
        print(f"❌ Error recreating table: {e}")
        print("Please run the SQL manually using the instructions above")

if __name__ == "__main__":
    recreate_optimized_table() 