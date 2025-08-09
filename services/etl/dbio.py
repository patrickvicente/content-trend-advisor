import json
from typing import List, Tuple
import psycopg

def get_conn(dsn: str) -> psycopg.Connection:
    """
    Get a connection to the database.
    """
    return psycopg.connect(dsn)

def insert_raw_row(conn, source, external_id, payload) -> None:
    """
    Insert a raw row into the database.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_content (source, external_id, payload)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (source, external_id, json.dumps(payload))
        )
        conn.commit()

def insert_many_raw_rows(conn, rows: List[Tuple[str, str, dict]]) -> int:
    """
    Insert multiple raw rows into the database
    returns number of rows inserted
    """
    # Convert dict payloads to JSON strings
    json_rows = [(source, external_id, json.dumps(payload)) 
                 for source, external_id, payload in rows]
    
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO raw_content (source, external_id, payload)
            VALUES (%s, %s, %s)
            ON CONFLICT (source, external_id) DO NOTHING
            """,
            json_rows
        )
        conn.commit()
        return cur.rowcount
    
def select_recent_external_ids(conn, source: str, since_days: int) -> set[str]:
    """
    Return a set of external_ids seen recently for dedupe in ETL
    
    Args:
        conn: Database connection
        source: Source identifier (e.g., 'youtube', 'reddit')
        since_days: Number of days to look back
        
    Returns:
        set[str]: Set of external_ids fetched within the time window
        
    Example:
        recent_ids = select_recent_external_ids(conn, 'youtube', 7)
        if video_id not in recent_ids:
            # Safe to fetch this video
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT external_id
            FROM raw_content
            WHERE source = %s AND fetched_at >= NOW() - INTERVAL '%s days'
            """,
            (source, since_days)
        )
        return set(row[0] for row in cur.fetchall())
