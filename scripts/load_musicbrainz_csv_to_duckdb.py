import argparse
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv


DEFAULT_DB_PATH = "warehouse/music.duckdb"


def init_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("create schema if not exists raw")
    conn.execute(
        """
        create table if not exists raw.musicbrainz_isrc_candidates (
            track_id varchar,
            isrc varchar,
            recording_mbid varchar,
            match_rank integer,
            matched_at varchar,
            isrc_response_json varchar
        )
        """
    )
    conn.execute(
        """
        create table if not exists raw.musicbrainz_recording_payloads (
            recording_mbid varchar,
            payload_json varchar,
            last_seen_at varchar
        )
        """
    )
    conn.execute(
        """
        create table if not exists raw.musicbrainz_work_payloads (
            work_mbid varchar,
            payload_json varchar,
            last_seen_at varchar
        )
        """
    )


def _load_csv_to_temp(conn: duckdb.DuckDBPyConnection, temp_name: str, csv_path: Path) -> bool:
    if not csv_path.exists():
        print(f"Skipping missing file: {csv_path}")
        return False

    conn.execute(f"drop table if exists {temp_name}")
    conn.execute(
        f"create temp table {temp_name} as select * from read_csv_auto(?, header=true)",
        [str(csv_path)],
    )
    row_count = conn.execute(f"select count(*) from {temp_name}").fetchone()[0]
    print(f"Loaded {row_count} rows from {csv_path.name}")
    return row_count > 0


def load_isrc_candidates(conn: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    if not _load_csv_to_temp(conn, "tmp_mb_isrc_candidates", csv_path):
        return

    conn.execute(
        """
        delete from raw.musicbrainz_isrc_candidates
        where track_id in (select distinct track_id from tmp_mb_isrc_candidates)
        """
    )
    conn.execute(
        """
        insert into raw.musicbrainz_isrc_candidates
        select
            track_id,
            upper(trim(isrc)) as isrc,
            recording_mbid,
            cast(match_rank as integer) as match_rank,
            matched_at,
            isrc_response_json
        from tmp_mb_isrc_candidates
        """
    )


def load_recording_payloads(conn: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    if not _load_csv_to_temp(conn, "tmp_mb_recording_payloads", csv_path):
        return

    conn.execute(
        """
        delete from raw.musicbrainz_recording_payloads
        where recording_mbid in (select distinct recording_mbid from tmp_mb_recording_payloads)
        """
    )
    conn.execute(
        """
        insert into raw.musicbrainz_recording_payloads
        select
            recording_mbid,
            payload_json,
            last_seen_at
        from tmp_mb_recording_payloads
        """
    )


def load_work_payloads(conn: duckdb.DuckDBPyConnection, csv_path: Path) -> None:
    if not _load_csv_to_temp(conn, "tmp_mb_work_payloads", csv_path):
        return

    conn.execute(
        """
        delete from raw.musicbrainz_work_payloads
        where work_mbid in (select distinct work_mbid from tmp_mb_work_payloads)
        """
    )
    conn.execute(
        """
        insert into raw.musicbrainz_work_payloads
        select
            work_mbid,
            payload_json,
            last_seen_at
        from tmp_mb_work_payloads
        """
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load MusicBrainz enrichment CSV outputs into DuckDB raw tables."
    )
    parser.add_argument(
        "--input-dir",
        default="exports/musicbrainz/results",
        help="Directory containing enrichment CSV outputs.",
    )
    args = parser.parse_args()

    load_dotenv()
    db_path = os.getenv("DUCKDB_PATH", DEFAULT_DB_PATH)

    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    conn = duckdb.connect(db_path)
    try:
        init_tables(conn)
        conn.execute("begin transaction")
        load_isrc_candidates(conn, input_dir / "musicbrainz_isrc_candidates.csv")
        load_recording_payloads(conn, input_dir / "musicbrainz_recording_payloads.csv")
        load_work_payloads(conn, input_dir / "musicbrainz_work_payloads.csv")
        conn.execute("commit")
    except Exception:
        conn.execute("rollback")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
