import os

import duckdb


DEFAULT_DB_PATH = "warehouse/music.duckdb"


def main() -> None:
    db_path = os.getenv("DUCKDB_PATH", DEFAULT_DB_PATH)
    conn = None
    try:
        conn = duckdb.connect(db_path)
        row = conn.execute(
            "select cast(max(added_at) as varchar) from analytics.stg_spotify_saved_tracks"
        ).fetchone()
        value = row[0] if row else None
        print(value or "")
    except Exception:
        # First run or missing analytics view should not fail the pipeline.
        print("")
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
