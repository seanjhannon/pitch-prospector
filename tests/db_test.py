import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

host = os.environ["SUPABASE_DB_HOST"]
port = os.environ.get("SUPABASE_DB_PORT", 5432)
db = os.environ["SUPABASE_DB_NAME"]
user = os.environ["SUPABASE_DB_USER"]
password = os.environ["SUPABASE_DB_PASSWORD"]

def test_connection():
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password
        )
        print("✅ Successfully connected to Supabase/Postgres DB!")
        conn.close()
    except Exception as e:
        print(f"❌ Failed to connect to Supabase/Postgres DB: {e}")

if __name__ == "__main__":
    test_connection()