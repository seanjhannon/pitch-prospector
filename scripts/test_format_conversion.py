#!/usr/bin/env python3
"""
Quick test to verify format conversion works correctly.
"""

def test_format_conversion():
    """Test converting from tuples to lists format."""
    print("🔍 Testing format conversion...")
    
    # Test input (what the app receives)
    input_sequence = [('FF', 'hit_into_play')]
    print(f"Input sequence: {input_sequence}")
    print(f"Input type: {type(input_sequence)}")
    print(f"First element type: {type(input_sequence[0])}")
    
    # Convert to database format
    search_sequence = [[pitch_type, outcome] for pitch_type, outcome in input_sequence]
    print(f"Converted sequence: {search_sequence}")
    print(f"Converted type: {type(search_sequence)}")
    print(f"First element type: {type(search_sequence[0])}")
    
    # Test matching
    database_sequence = [['FF', 'hit_into_play']]
    print(f"Database sequence: {database_sequence}")
    
    print(f"Input matches database: {input_sequence == database_sequence}")
    print(f"Converted matches database: {search_sequence == database_sequence}")
    
    # Test with multiple pitches
    print("\n🔍 Testing multi-pitch sequence...")
    input_multi = [('FF', 'swinging_strike'), ('FF', 'called_strike'), ('FF', 'called_strike')]
    converted_multi = [[pitch_type, outcome] for pitch_type, outcome in input_multi]
    database_multi = [['FF', 'swinging_strike'], ['FF', 'called_strike'], ['FF', 'called_strike']]
    
    print(f"Input multi: {input_multi}")
    print(f"Converted multi: {converted_multi}")
    print(f"Database multi: {database_multi}")
    print(f"Converted matches database: {converted_multi == database_multi}")

if __name__ == "__main__":
    print("🚀 Testing format conversion...")
    print("=" * 50)
    
    test_format_conversion()
    
    print("\n" + "=" * 50)
    print("🎉 Format conversion test completed!") 