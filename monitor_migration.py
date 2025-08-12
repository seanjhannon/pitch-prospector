#!/usr/bin/env python3
"""
Live migration progress monitor for CockroachDB import.
Updates every 15 seconds to show real-time progress.
"""

import psycopg
import os
import time
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

def check_migration_progress():
    """Check current migration progress."""
    try:
        host = '34.94.157.111'
        user = os.getenv('COCKROACH_USER')
        password = os.getenv('COCKROACH_PASSWORD')
        database = 'pitches-8229.defaultdb'
        
        dsn = f'postgresql://{user}:{password}@{host}:26257/{database}?sslmode=require'
        
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT COUNT(*) FROM atbats_optimized;')
                count = cur.fetchone()[0]
                
                # Calculate progress
                total_expected = 1857706
                percentage = (count / total_expected) * 100
                remaining = total_expected - count
                
                # Clear screen and show progress
                os.system('clear' if os.name == 'posix' else 'cls')
                
                print("🐔 CockroachDB Migration Progress Monitor")
                print("=" * 50)
                print(f"⏰ Last Update: {datetime.now().strftime('%H:%M:%S')}")
                print(f"📊 Rows Imported: {count:,}")
                print(f"🎯 Total Expected: {total_expected:,}")
                print(f"📈 Progress: {percentage:.1f}%")
                print(f"⏳ Remaining: {remaining:,} rows")
                print()
                
                # Progress bar
                bar_length = 40
                filled_length = int(bar_length * count // total_expected)
                bar = '█' * filled_length + '░' * (bar_length - filled_length)
                print(f"Progress: [{bar}] {percentage:.1f}%")
                print()
                
                # Estimated time remaining (rough calculation)
                if count > 0:
                    # Assume linear progress - adjust this based on actual performance
                    print("⏱️  Note: Time estimates are approximate")
                
                print("🔄 Updating every 15 seconds... (Ctrl+C to stop)")
                
                return count, percentage
                
    except Exception as e:
        print(f"❌ Error checking progress: {e}")
        return 0, 0

def main():
    """Main monitoring loop."""
    print("�� Starting migration progress monitor...")
    print("Press Ctrl+C to stop monitoring")
    print()
    
    try:
        while True:
            count, percentage = check_migration_progress()
            
            # Check if migration is complete
            if count >= 1857706:
                print("�� MIGRATION COMPLETE! All 1,857,706 rows imported!")
                break
            
            # Wait 15 seconds
            time.sleep(15)
            
    except KeyboardInterrupt:
        print("\n�� Monitoring stopped by user")
        print("Migration may still be running in another terminal")

if __name__ == "__main__":
    main()
