#!/usr/bin/env python3
"""
Performance test script to measure database query improvements.
Tests both the old and new query methods to compare performance.
"""

import time
import hashlib
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pitch_prospector.db import (
    get_atbats_by_date_range, 
    get_atbats_by_date_and_sequence,
    get_atbats_by_sequence_hash
)

load_dotenv()

def test_count_query():
    """Test the basic COUNT query performance."""
    print("🔍 Testing COUNT query performance...")
    
    start_time = time.time()
    from pitch_prospector.db_pool import get_db_connection
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM atbats_simple")
            result = cur.fetchone()
            count = result[0] if result else 0
    
    duration = time.time() - start_time
    print(f"  📊 Found {count:,} records in {duration:.3f}s")
    return duration

def test_date_range_query():
    """Test date range query performance."""
    print("🔍 Testing date range query performance...")
    
    # Test a 30-day range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    start_time = time.time()
    results = get_atbats_by_date_range(str(start_date.date()), str(end_date.date()))
    duration = time.time() - start_time
    
    print(f"  📊 Found {len(results):,} at-bats in {duration:.3f}s")
    return duration, len(results)

def test_sequence_hash_query():
    """Test sequence hash query performance."""
    print("🔍 Testing sequence hash query performance...")
    
    # Create a test sequence hash
    test_sequence = (("FF", "called_strike"), ("SL", "ball"), ("FF", "swinging_strike"))
    hash_input = str(test_sequence).encode("utf-8")
    sequence_hash = hashlib.sha1(hash_input).hexdigest()
    
    start_time = time.time()
    results = get_atbats_by_sequence_hash(sequence_hash)
    duration = time.time() - start_time
    
    print(f"  📊 Found {len(results):,} at-bats in {duration:.3f}s")
    return duration, len(results)

def test_optimized_query():
    """Test the optimized combined query performance."""
    print("🔍 Testing optimized combined query performance...")
    
    # Test a 30-day range with sequence hash
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    test_sequence = (("FF", "called_strike"), ("SL", "ball"), ("FF", "swinging_strike"))
    hash_input = str(test_sequence).encode("utf-8")
    sequence_hash = hashlib.sha1(hash_input).hexdigest()
    
    start_time = time.time()
    results = get_atbats_by_date_and_sequence(str(start_date.date()), str(end_date.date()), sequence_hash)
    duration = time.time() - start_time
    
    print(f"  📊 Found {len(results):,} at-bats in {duration:.3f}s")
    return duration, len(results)

def compare_old_vs_new_approach():
    """Compare the old approach vs new optimized approach."""
    print("🔍 Comparing old vs new query approaches...")
    
    # Test parameters
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    test_sequence = (("FF", "called_strike"), ("SL", "ball"), ("FF", "swinging_strike"))
    hash_input = str(test_sequence).encode("utf-8")
    sequence_hash = hashlib.sha1(hash_input).hexdigest()
    
    # Old approach: fetch all data, then filter in Python
    print("  📊 Testing OLD approach (fetch all, filter in Python)...")
    old_start = time.time()
    all_atbats = get_atbats_by_date_range(str(start_date.date()), str(end_date.date()))
    old_matches = [row for row in all_atbats if row["pitch_sequence_hash"] == sequence_hash]
    old_duration = time.time() - old_start
    
    # New approach: filter at database level
    print("  📊 Testing NEW approach (filter at database level)...")
    new_start = time.time()
    new_matches = get_atbats_by_date_and_sequence(str(start_date.date()), str(end_date.date()), sequence_hash)
    new_duration = time.time() - new_start
    
    print(f"  📊 OLD approach: {len(old_matches)} matches in {old_duration:.3f}s")
    print(f"  📊 NEW approach: {len(new_matches)} matches in {new_duration:.3f}s")
    print(f"  📊 Speed improvement: {old_duration/new_duration:.1f}x faster")
    
    return old_duration, new_duration, len(old_matches), len(new_matches)

if __name__ == "__main__":
    print("🚀 Starting performance tests...")
    print("=" * 50)
    
    # Test 1: COUNT query
    count_duration = test_count_query()
    print()
    
    # Test 2: Date range query
    date_duration, date_count = test_date_range_query()
    print()
    
    # Test 3: Sequence hash query
    seq_duration, seq_count = test_sequence_hash_query()
    print()
    
    # Test 4: Optimized combined query
    opt_duration, opt_count = test_optimized_query()
    print()
    
    # Test 5: Compare approaches
    old_dur, new_dur, old_count, new_count = compare_old_vs_new_approach()
    print()
    
    print("=" * 50)
    print("📊 Performance Summary:")
    print(f"  • COUNT query: {count_duration:.3f}s")
    print(f"  • Date range query: {date_duration:.3f}s ({date_count:,} results)")
    print(f"  • Sequence hash query: {seq_duration:.3f}s ({seq_count:,} results)")
    print(f"  • Optimized combined query: {opt_duration:.3f}s ({opt_count:,} results)")
    print(f"  • Old vs New approach: {old_dur:.3f}s vs {new_dur:.3f}s ({old_dur/new_dur:.1f}x improvement)")
    
    if count_duration < 1.0:
        print("  ✅ COUNT query performance is good (< 1s)")
    else:
        print("  ⚠️  COUNT query performance needs improvement")
    
    if old_dur/new_dur > 2.0:
        print("  ✅ Query optimization is working well (> 2x improvement)")
    else:
        print("  ⚠️  Query optimization could be improved") 