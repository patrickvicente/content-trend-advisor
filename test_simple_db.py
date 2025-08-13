#!/usr/bin/env python3
"""
Very simple database test to isolate the constraint issue.
"""

import os
import psycopg

def load_env_file():
    """Load environment variables from .env file."""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Remove quotes if present
                    value = value.strip('"\'')
                    os.environ[key] = value
        print("✅ Loaded .env file")
        print(f"   DATABASE_URL: {os.getenv('DATABASE_URL')}")
    else:
        print("⚠️ .env file not found")

def test_simple():
    """Test simple database operations."""
    try:
        dsn = os.getenv("DATABASE_URL")
        print(f"🔗 Connecting with DSN: {dsn}")
        
        conn = psycopg.connect(dsn)
        print("✅ Connected to database")
        
        # Check current database and schema
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_schema();")
            db, schema = cur.fetchone()
            print(f"📊 Database: {db}, Schema: {schema}")
            
            # Check if table exists
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'raw_content');")
            table_exists = cur.fetchone()[0]
            print(f"📋 Table exists: {table_exists}")
            
            # Check constraints
            cur.execute("SELECT conname, contype FROM pg_constraint WHERE conrelid = 'raw_content'::regclass;")
            constraints = cur.fetchall()
            print(f"🔒 Constraints: {constraints}")
            
            # Try a simple insert without ON CONFLICT
            cur.execute("INSERT INTO public.raw_content (source, external_id, payload) VALUES (%s, %s, %s);", 
                       ("test_simple", "test_id_simple", '{"test": "simple"}'))
            print("✅ Simple insert successful")
            
            # Try the ON CONFLICT insert
            cur.execute("""
                INSERT INTO public.raw_content (source, external_id, payload) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (source, external_id) DO NOTHING;
                """, 
                ("test_simple", "test_id_simple", '{"test": "simple_updated"}'))
            print("✅ ON CONFLICT insert successful")
            
            conn.commit()
        
        conn.close()
        print("🎉 All tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    load_env_file()
    test_simple()

