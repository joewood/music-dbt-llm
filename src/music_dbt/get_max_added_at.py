import os

import duckdb


DEFAULT_DB_PATH = "warehouse/music.duckdb"


def get_max_added_at_value(db_path: str | None = None) -> str:
    target_db_path = db_path or os.getenv("DUCKDB_PATH", DEFAULT_DB_PATH)
    conn = None
    try:
        conn = duckdb.connect(target_db_path)
        row = conn.execute(
            "select cast(max(added_at) as varchar) from analytics.stg_spotify_saved_tracks"
        ).fetchone()
        value = row[0] if row else None
        return value or ""
    except Exception:
        # First run or missing analytics view should not fail the pipeline.
        return ""
    finally:
        if conn is not None:
            conn.close()


def main() -> None:
    print(get_max_added_at_value())


if __name__ == "__main__":
    main()
