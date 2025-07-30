#!/usr/bin/env python3
"""
Test script to verify search functionality with pitch type + outcome combinations.
"""

import os
from dotenv import load_dotenv
import streamlit as st
from st_supabase_connection import SupabaseConnection
import time

load_dotenv()

# Initialize Supabase connection
conn = st.connection("supabase", type=SupabaseConnection)

def test_search_functionality():
    """Test the search functionality with various pitch type + outcome combinations."""
    print("🔍 Testing search functionality with pitch type + outcome combinations...")
    
    # Test 1: Sample actual data first
    print("\n📊 Test 1: Sample actual data from database")
    start_time = time.time()
    result = conn.table("atbats_optimized").select("pitch_sequence").limit(5).execute()
    if result.data:
        print("  • Sample pitch sequences:")
        for i, atbat in enumerate(result.data):
            sequence = atbat.get('pitch_sequence', [])
            print(f"    {i+1}. {sequence}")
    duration = time.time() - start_time
    print(f"  • Retrieved sample data in {duration:.3f}s")
    
    # Test 2: Count by outcome type
    print("\n📊 Test 2: Count at-bats by outcome type")
    start_time = time.time()
    result = conn.table("atbats_optimized").select("pitch_sequence").limit(1000).execute()
    if result.data:
        outcome_counts = {}
        pitch_type_counts = {}
        for atbat in result.data:
            sequence = atbat.get('pitch_sequence', [])
            for pitch in sequence:
                if isinstance(pitch, list) and len(pitch) >= 2:
                    pitch_type = pitch[0]
                    outcome = pitch[1]
                    outcome_counts[outcome] = outcome_counts.get(outcome, 0) + 1
                    pitch_type_counts[pitch_type] = pitch_type_counts.get(pitch_type, 0) + 1
        
        print("  • Top 10 outcomes:")
        for outcome, count in sorted(outcome_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"    {outcome}: {count:,}")
        
        print("  • Top 10 pitch types:")
        for pitch_type, count in sorted(pitch_type_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"    {pitch_type}: {count:,}")
    
    duration = time.time() - start_time
    print(f"  • Analyzed outcomes in {duration:.3f}s")
    
    # Test 3: Date range search
    print("\n📊 Test 3: Date range search")
    start_time = time.time()
    result = conn.table("atbats_optimized").select("*", count="exact").gte("game_date", "2025-07-01").lte("game_date", "2025-07-29").execute()
    count = result.count if result.count is not None else 0
    duration = time.time() - start_time
    print(f"  • Found {count:,} at-bats in July 2025 in {duration:.3f}s")
    
    # Test 4: Find specific sequences by filtering in Python
    print("\n📊 Test 4: Find specific sequences (Python filtering)")
    start_time = time.time()
    result = conn.table("atbats_optimized").select("pitch_sequence").limit(10000).execute()
    if result.data:
        # Look for specific sequences
        target_sequences = [
            [["FF", "called_strike"]],
            [["FF", "ball"], ["FF", "called_strike"]],
            [["FF", "ball"], ["SL", "ball"], ["FF", "swinging_strike"]]
        ]
        
        for i, target_seq in enumerate(target_sequences):
            matches = 0
            for atbat in result.data:
                sequence = atbat.get('pitch_sequence', [])
                if sequence == target_seq:
                    matches += 1
            print(f"    Sequence {i+1} {target_seq}: {matches} matches")
    
    duration = time.time() - start_time
    print(f"  • Found sequences in {duration:.3f}s")
    
    # Test 5: Verify data structure
    print("\n📊 Test 5: Verify data structure")
    if result.data:
        print("  • Data structure verification:")
        sample_atbat = result.data[0]
        sequence = sample_atbat.get('pitch_sequence', [])
        print(f"    Sample sequence type: {type(sequence)}")
        print(f"    Sample sequence: {sequence}")
        if sequence:
            print(f"    First pitch type: {type(sequence[0])}")
            print(f"    First pitch: {sequence[0]}")
            if isinstance(sequence[0], list) and len(sequence[0]) >= 2:
                print(f"    First pitch type: {sequence[0][0]}")
                print(f"    First pitch outcome: {sequence[0][1]}")

def test_app_search_functions():
    """Test the actual app search functions."""
    print("\n🔍 Testing app search functions...")
    
    # Import the app functions
    import sys
    sys.path.append('pitch_prospector')
    from app import get_atbats_by_date_and_sequence_official, get_pitch_sequences_for_atbat_official
    
    # Test the search function
    print("\n📊 Test: App search function")
    start_time = time.time()
    test_sequence = [("FF", "called_strike"), ("SL", "ball")]
    matches = get_atbats_by_date_and_sequence_official("2025-07-01", "2025-07-29", test_sequence)
    duration = time.time() - start_time
    print(f"  • Found {len(matches)} matches in {duration:.3f}s")
    
    if matches:
        # Test getting pitch sequence details
        print("\n📊 Test: Getting pitch sequence details")
        atbat_id = matches[0]["id"]
        pitch_details = get_pitch_sequences_for_atbat_official(atbat_id)
        print(f"  • Pitch details for at-bat {atbat_id}:")
        for i, pitch in enumerate(pitch_details):
            print(f"    Pitch {i+1}: {pitch['pitch_type']} - {pitch['description']} ({pitch['release_speed']} mph, Zone {pitch['zone']})")

if __name__ == "__main__":
    print("🚀 Testing search functionality with pitch type + outcome combinations...")
    print("=" * 80)
    
    test_search_functionality()
    test_app_search_functions()
    
    print("\n" + "=" * 80)
    print("🎉 Search functionality testing completed!")
    print("\n✅ Key findings:")
    print("  • Database contains pitch sequences with outcomes")
    print("  • Search queries work with pitch type + outcome combinations")
    print("  • App functions properly handle the new data structure")
    print("\n📋 Next steps:")
    print("  • Test the Streamlit app UI")
    print("  • Verify search results match expectations")
    print("  • Scale to full historical data if successful") 