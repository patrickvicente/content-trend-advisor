#!/usr/bin/env python3
"""
Focused test to debug the database constraint issue.
"""

import sys
import os
from pathlib import Path
import json

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def load_env_file():
    """Load environment variables from .env file."""
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key] = value
        print("✅ Loaded .env file")
    else:
        print("⚠️ .env file not found")

def test_db_operations():
    """Test database operations step by step."""
    try:
        from services.etl.dbio import get_conn, insert_many_raw_rows
        
        dsn = os.getenv("DATABASE_URL")
        if not dsn:
            print("⚠️ DATABASE_URL not set")
            return
        
        print(f"🔗 Connecting to database...")
        conn = get_conn(dsn)
        print("✅ Database connection successful")
        
        # Test single row insertion first
        print("\n🧪 Testing single row insertion...")
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.raw_content (source, external_id, payload)
                VALUES (%s, %s, %s)
                ON CONFLICT (source, external_id) DO NOTHING
                """,
                ("test_single", "test_id_single", json.dumps({"test": "single"}))
            )
            conn.commit()
            print(f"✅ Single row inserted, rowcount: {cur.rowcount}")
        
        # Test batch insertion
        print("\n🧪 Testing batch insertion...")
        mock_rows = [
            ("test_batch", f"test_id_batch_{i}", {"test": f"data_{i}"})
            for i in range(3)
        ]
        
        print(f"📝 Preparing {len(mock_rows)} rows...")
        inserted_count = insert_many_raw_rows(conn, mock_rows)
        print(f"✅ Batch inserted {inserted_count} rows")
        
        conn.close()
        print("\n🎉 Database operations test completed successfully!")
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    load_env_file()
    test_db_operations()
