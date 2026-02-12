#!/usr/bin/env python3
"""Parallel backfill: multiprocessing by file + batch inserts into TimescaleDB."""
import json
import os
import glob
from datetime import datetime, timezone
from multiprocessing import Pool, cpu_count
import psycopg2
from psycopg2.extras import execute_values, Json

DATA_DIR = os.environ.get("DATA_DIR", "/data")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10000"))
N_WORKERS = int(os.environ.get("N_WORKERS", "0")) or max(1, cpu_count() - 1)
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "20"))  # fichiers par worker

PG_CONN = (
    f"host={os.environ.get('PGHOST')} port={os.environ.get('PGPORT', 5432)} "
    f"dbname={os.environ.get('PGDATABASE')} user={os.environ.get('PGUSER')} "
    f"password={os.environ.get('PGPASSWORD')}"
)

INSERT_SQL = """
    INSERT INTO tweets (ts, id_str, lang, source, screen_name, place,
        quote_count, favorited, coordinates, entities, friends_count, user_id) VALUES %s
    ON CONFLICT (ts, id_str) DO NOTHING
"""


def parse_ts(obj):
    ms = obj.get("timestamp_ms")
    if ms is not None:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)
    created = obj.get("created_at")
    if created:
        return datetime.strptime(created, "%a %b %d %H:%M:%S %z %Y")
    return None


def extract_row(obj):
    """Extrait ts, id_str, lang, source, screen_name, place, quote_count, favorited, coordinates, entities, friends_count, user_id."""
    user = obj.get("user") or {}
    place = obj.get("place")
    place_name = place.get("name") if place and isinstance(place, dict) else None
    coords = obj.get("coordinates")
    entities_data = obj.get("entities")
    return (
        obj.get("lang") or "und",
        (obj.get("source") or "")[:100],
        user.get("screen_name"),
        place_name,
        obj.get("quote_count"),
        obj.get("favorited"),
        Json(coords) if coords and isinstance(coords, dict) else None,
        Json(entities_data) if entities_data and isinstance(entities_data, dict) else None,
        user.get("friends_count"),
        user.get("id"),
    )


def ensure_schema(conn):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'tweets' AND column_name = 'data'
        """
    )
    if cur.fetchone() is not None:
        cur.execute("DROP TABLE tweets CASCADE")
        conn.commit()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tweets (
            ts TIMESTAMPTZ NOT NULL,
            id_str TEXT NOT NULL,
            lang TEXT,
            source TEXT,
            screen_name TEXT,
            place TEXT,
            quote_count INT,
            favorited BOOLEAN,
            coordinates JSONB,
            entities JSONB,
            friends_count INT,
            user_id BIGINT,
            UNIQUE(ts, id_str)
        );
    """
    )
    for col, typ in [
        ("quote_count", "INT"),
        ("favorited", "BOOLEAN"),
        ("coordinates", "JSONB"),
        ("entities", "JSONB"),
        ("friends_count", "INT"),
        ("user_id", "BIGINT"),
    ]:
        cur.execute(
            f"ALTER TABLE tweets ADD COLUMN IF NOT EXISTS {col} {typ}"
        )
    cur.execute(
        """
        SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'tweets';
    """
    )
    if cur.fetchone() is None:
        cur.execute("SELECT create_hypertable('tweets', 'ts', if_not_exists => true);")
    conn.commit()
    cur.close()


def process_files(args):
    file_paths, pg_conn = args
    conn = psycopg2.connect(pg_conn)
    cur = conn.cursor()
    batch = []
    inserted = 0
    skipped = 0
    read_count = 0
    for path in file_paths:
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                read_count += 1
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
                row = extract_row(obj)
                batch.append((ts, id_str, *row))
                if len(batch) >= BATCH_SIZE:
                    execute_values(
                        cur, INSERT_SQL, batch,
                        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    )
                    inserted += cur.rowcount
                    conn.commit()
                    batch = []
    if batch:
        execute_values(cur, INSERT_SQL, batch, template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        inserted += cur.rowcount
        conn.commit()
    cur.close()
    conn.close()
    return inserted, skipped, read_count


def main():
    conn = psycopg2.connect(PG_CONN)
    ensure_schema(conn)
    conn.close()

    pattern = os.path.join(DATA_DIR, "raw*.json")
    files = [f for f in glob.glob(pattern) if not f.endswith("Zone.Identifier")]
    files.sort()
    max_files = int(os.environ.get("MAX_FILES", "0")) or len(files)
    files = files[:max_files]
    if not files:
        print("No files found.")
        return

    chunk_size = min(CHUNK_SIZE, len(files))
    chunks = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]
    worker_args = [(c, PG_CONN) for c in chunks]
    n = min(N_WORKERS, len(chunks))

    print(f"Workers: {n}, batch: {BATCH_SIZE}, files: {len(files)}, chunks: {len(chunks)}", flush=True)
    with Pool(n) as pool:
        results = pool.map(process_files, worker_args)
    total_inserted = sum(r[0] for r in results)
    total_skipped = sum(r[1] for r in results)
    total_read = sum(r[2] for r in results)
    print(
        f"Done. Read: {total_read}, inserted: {total_inserted}, skipped: {total_skipped}"
    )


if __name__ == "__main__":
    main()
