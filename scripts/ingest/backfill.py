#!/usr/bin/env python3
"""One-time backfill: read NDJSON from /data (json-data/raw), insert into TimescaleDB."""
import json
import os
import glob
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import Json

DATA_DIR = os.environ.get("DATA_DIR", "/data")
PG_CONN = (
    f"host={os.environ.get('PGHOST')} port={os.environ.get('PGPORT', 5432)} "
    f"dbname={os.environ.get('PGDATABASE')} user={os.environ.get('PGUSER')} "
    f"password={os.environ.get('PGPASSWORD')}"
)


def parse_ts(obj):
    """Extract timestamp (UTC) from tweet. Returns None if missing."""
    ms = obj.get("timestamp_ms")
    if ms is not None:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)
    created = obj.get("created_at")
    if created:
        return datetime.strptime(created, "%a %b %d %H:%M:%S %z %Y")
    return None


def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tweets (
            ts TIMESTAMPTZ NOT NULL,
            id_str TEXT NOT NULL,
            data JSONB NOT NULL,
            UNIQUE(ts, id_str)
        );
    """)
    cur.execute("""
        SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'tweets';
    """)
    if cur.fetchone() is None:
        cur.execute("SELECT create_hypertable('tweets', 'ts', if_not_exists => true);")
    conn.commit()
    cur.close()


def main():
    conn = psycopg2.connect(PG_CONN)
    ensure_schema(conn)

    pattern = os.path.join(DATA_DIR, "raw*.json")
    files = [f for f in glob.glob(pattern) if not f.endswith("Zone.Identifier")]
    files.sort()

    cur = conn.cursor()
    inserted = 0
    skipped = 0
    for path in files:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    skipped += 1
                    continue
                ts = parse_ts(obj)
                if ts is None:
                    skipped += 1
                    continue
                id_str = obj.get("id_str") or str(obj.get("id", ""))
                if not id_str:
                    skipped += 1
                    continue
                try:
                    cur.execute(
                        "INSERT INTO tweets (ts, id_str, data) VALUES (%s, %s, %s) ON CONFLICT (ts, id_str) DO NOTHING",
                        (ts, id_str, Json(obj)),
                    )
                    if cur.rowcount:
                        inserted += 1
                except psycopg2.IntegrityError:
                    skipped += 1
                    conn.rollback()
                    continue
        conn.commit()
        print(f"  {os.path.basename(path)}", flush=True)

    conn.commit()
    cur.close()
    conn.close()
    print(f"Done. Inserted: {inserted}, skipped: {skipped}")


if __name__ == "__main__":
    main()
