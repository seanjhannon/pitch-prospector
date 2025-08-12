#!/bin/bash
# Manual migration commands for Pitch Prospector
# Run these commands one by one for maximum control

echo "🚀 Pitch Prospector Migration Commands"
echo "======================================"
echo ""

# Step 1: Check if PostgreSQL tools are installed
echo "🔍 Step 1: Checking dependencies..."
if command -v pg_dump &> /dev/null; then
    echo "✅ pg_dump found: $(pg_dump --version)"
else
    echo "❌ pg_dump not found. Please install PostgreSQL client tools:"
    echo "   macOS: brew install postgresql"
    echo "   Ubuntu: sudo apt-get install postgresql-client"
    exit 1
fi

if command -v psql &> /dev/null; then
    echo "✅ psql found: $(psql --version)"
else
    echo "❌ psql not found. Please install PostgreSQL client tools."
    exit 1
fi

echo ""

# Step 2: Dump from Supabase
echo "📥 Step 2: Dump data from Supabase"
echo "Run this command (replace with your actual Supabase credentials):"
echo ""
echo "pg_dump --data-only --table=atbats_optimized --no-owner --no-privileges --verbose --file=supabase_dump.sql postgresql://postgres:[YOUR-PASSWORD]@db.xjjwtmcoklsqosxkexqw.supabase.co:5432/postgres"
echo ""

# Step 3: Check dump file
echo "📁 Step 3: Check the dump file"
echo "Run this command after the dump completes:"
echo ""
echo "ls -lh supabase_dump.sql"
echo ""

# Step 4: Restore to CockroachDB
echo "📤 Step 4: Restore to CockroachDB"
echo "Run this command (uses your CA certificate):"
echo ""
echo "psql --echo-all --verbose --file=supabase_dump.sql postgresql://sean:_GS_iQHq4ZjjvwA4-VBqcQ@pitches-8229.jxf.gcp-us-west2.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full&sslrootcert=~/.postgresql/root.crt"
echo ""

# Step 5: Verify migration
echo "🔍 Step 5: Verify migration"
echo "Run this command to check the record count:"
echo ""
echo "psql postgresql://sean:_GS_iQHq4ZjjvwA4-VBqcQ@pitches-8229.jxf.gcp-us-west2.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full&sslrootcert=~/.postgresql/root.crt -c 'SELECT COUNT(*) FROM atbats_optimized;'"
echo ""

# Step 6: Cleanup
echo "🧹 Step 6: Cleanup (optional)"
echo "Run this command to remove the dump file:"
echo ""
echo "rm supabase_dump.sql"
echo ""

echo "======================================"
echo "🎯 Migration Steps Complete!"
echo ""
echo "💡 Tips:"
echo "  - Run commands one at a time"
echo "  - Check for errors after each step"
echo "  - Keep the dump file until you verify the migration"
echo "  - Monitor the progress with --verbose flags"
echo ""
echo "📊 Expected Performance:"
echo "  - Dump: 2-10 minutes (depending on data size)"
echo "  - Restore: 5-20 minutes (depending on network/DB performance)"
echo "  - Total: 7-30 minutes for 675k+ records" 