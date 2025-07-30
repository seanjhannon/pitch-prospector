#!/usr/bin/env python3
"""
Quick test to verify the new search approach works.
"""

import os
from dotenv import load_dotenv
import streamlit as st
from st_supabase_connection import SupabaseConnection
import time

load_dotenv()

# Initialize Supabase connection
conn = st.connection("supabase", type=SupabaseConnection)

def test_new_search_approach():
    """Test the new search approach with Python filtering."""
    print("🔍 Testing new search approach with Python filtering...")
    
    # Test 1: Get data in date range (get more data)
    print("\n📊 Test 1: Get data in date range")
    start_time = time.time()
    result = conn.table("atbats_optimized").select(
        "id, game_pk, at_bat_number, game_date, batter, pitcher, inning, pitch_sequence"
    ).gte("game_date", "2025-07-01").lte("game_date", "2025-07-29").limit(10000).execute()
    
    total_atbats = len(result.data) if result.data else 0
    duration = time.time() - start_time
    print(f"  • Found {total_atbats:,} at-bats in July 2025 in {duration:.3f}s")
    
    if not result.data:
        print("  ❌ No data found")
        return
    
    # Test 2: Show some actual sequences to understand the data
    print("\n📊 Test 2: Show actual sequences from database")
    print("  • Sample sequences:")
    for i, atbat in enumerate(result.data[:5]):
        sequence = atbat.get('pitch_sequence', [])
        print(f"    {i+1}. {sequence}")
    
    # Test 3: Find sequences that start with FF
    print("\n📊 Test 3: Find sequences starting with FF")
    ff_sequences = []
    for atbat in result.data:
        sequence = atbat.get('pitch_sequence', [])
        if sequence and len(sequence) > 0 and isinstance(sequence[0], list) and len(sequence[0]) > 0 and sequence[0][0] == "FF":
            ff_sequences.append(sequence)
    
    print(f"  • Found {len(ff_sequences)} sequences starting with FF")
    if ff_sequences:
        print("  • Sample FF sequences:")
        for i, seq in enumerate(ff_sequences[:3]):
            print(f"    {i+1}. {seq}")
    
    # Test 4: Find specific sequences that actually exist
    print("\n📊 Test 4: Find specific sequences that actually exist")
    test_sequences = [
        [["FF", "hit_into_play"]],  # This exists (35 occurrences)
        [["SI", "hit_into_play"]],  # This exists (23 occurrences)
        [["FF", "swinging_strike"], ["FF", "called_strike"], ["FF", "called_strike"]],  # This exists
        [["FF", "swinging_strike"], ["FF", "foul"], ["FF", "foul"], ["FF", "ball"], ["CH", "blocked_ball"], ["FF", "ball"]]  # This exists
    ]
    
    for i, test_sequence in enumerate(test_sequences):
        start_time = time.time()
        matches = 0
        
        for atbat in result.data:
            stored_sequence = atbat.get('pitch_sequence', [])
            if stored_sequence == test_sequence:
                matches += 1
        
        duration = time.time() - start_time
        print(f"  • Sequence {i+1} {test_sequence}: {matches} matches in {duration:.3f}s")
    
    # Test 5: Find any single-pitch sequences
    print("\n📊 Test 5: Find single-pitch sequences")
    single_pitch_sequences = {}
    for atbat in result.data:
        sequence = atbat.get('pitch_sequence', [])
        if len(sequence) == 1:
            seq_str = str(sequence)
            single_pitch_sequences[seq_str] = single_pitch_sequences.get(seq_str, 0) + 1
    
    print(f"  • Found {len(single_pitch_sequences)} unique single-pitch sequences")
    print("  • Top 5 single-pitch sequences:")
    for seq, count in sorted(single_pitch_sequences.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"    {seq}: {count} occurrences")

if __name__ == "__main__":
    print("🚀 Quick test of new search approach...")
    print("=" * 60)
    
    test_new_search_approach()
    
    print("\n" + "=" * 60)
    print("🎉 Quick test completed!") 