import argparse
import datetime as dt
import os
from typing import Dict, List, Tuple

import duckdb
from dotenv import load_dotenv
from spotipy import Spotify
from spotify_auth import build_pkce_spotify_client


DEFAULT_REDIRECT_URI = "http://127.0.0.1:8888/callback"


def normalize_release_date(release_date: str, precision: str) -> str:
    """Normalize Spotify's variable release date precision to YYYY-MM-DD."""
    if not release_date:
        return "1900-01-01"

    if precision == "year":
        return f"{release_date}-01-01"
    if precision == "month":
        return f"{release_date}-01"
    return release_date


def get_spotify_client() -> Spotify:
    required = ["SPOTIPY_CLIENT_ID"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )

    redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI", DEFAULT_REDIRECT_URI)
    scope = [
        "user-read-private",
        "user-read-email",
        "user-library-read",
        "playlist-read-private",
    ]

    return build_pkce_spotify_client(
        client_id=os.environ["SPOTIPY_CLIENT_ID"],
        redirect_uri=redirect_uri,
        scopes=scope,
        cache_path=".spotify_token_cache.json",
    )


def parse_iso8601_utc(ts: str | None) -> dt.datetime | None:
    """Parse an ISO-8601 timestamp (including trailing Z) into a UTC-aware datetime."""
    if not ts:
        return None

    normalized = ts.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    parsed = dt.datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def fetch_saved_tracks(
    client: Spotify,
    max_tracks: int | None = None,
    stop_before_added_at: dt.datetime | None = None,
) -> Tuple[List[Dict], List[Dict]]:
    limit = 50
    offset = 0
    tracks: Dict[str, Dict] = {}
    artists: List[Dict] = []
    run_ts = dt.datetime.now(dt.timezone.utc)
    stop_ingest = False

    while True:
        response = client.current_user_saved_tracks(limit=limit, offset=offset)
        items = response.get("items", [])
        if not items:
            break

        for item in items:
            saved_at = item.get("added_at")
            saved_at_dt = parse_iso8601_utc(saved_at)

            # Spotify saved tracks are returned newest first. Once we hit an older
            # row than our cutoff, we can stop paging immediately.
            if (
                stop_before_added_at is not None
                and saved_at_dt is not None
                and saved_at_dt < stop_before_added_at
            ):
                stop_ingest = True
                break

            track = item.get("track") or {}
            if not track:
                continue

            track_id = track.get("id")
            if not track_id:
                continue

            album = track.get("album") or {}
            release_date = normalize_release_date(
                album.get("release_date", "1900-01-01"),
                album.get("release_date_precision", "day"),
            )

            track_row = {
                "track_id": track_id,
                "added_at": saved_at,
                "track_name": track.get("name"),
                "isrc": (track.get("external_ids") or {}).get("isrc"),
                "album_id": album.get("id"),
                "album_name": album.get("name"),
                "album_release_date": release_date,
                "album_total_tracks": album.get("total_tracks"),
                "track_number": track.get("track_number"),
                "disc_number": track.get("disc_number"),
                "duration_ms": track.get("duration_ms"),
                "explicit": bool(track.get("explicit", False)),
                "popularity": track.get("popularity"),
                "is_local": bool(track.get("is_local", False)),
                "primary_artist_id": None,
                "primary_artist_name": None,
                "spotify_track_url": (
                    (track.get("external_urls") or {}).get("spotify")
                ),
                "ingest_run_at": run_ts.isoformat(),
            }

            artist_items = track.get("artists") or []
            if artist_items:
                track_row["primary_artist_id"] = artist_items[0].get("id")
                track_row["primary_artist_name"] = artist_items[0].get("name")

            tracks[track_id] = track_row

            for idx, artist in enumerate(artist_items):
                artists.append(
                    {
                        "track_id": track_id,
                        "artist_id": artist.get("id"),
                        "artist_name": artist.get("name"),
                        "artist_order": idx + 1,
                        "ingest_run_at": run_ts.isoformat(),
                    }
                )

            if max_tracks and len(tracks) >= max_tracks:
                break

        print(f"Fetched {len(tracks)} saved tracks so far...")

        if max_tracks and len(tracks) >= max_tracks:
            break

        if stop_ingest:
            break

        offset += limit

    track_rows = list(tracks.values())
    if max_tracks:
        track_rows = track_rows[:max_tracks]
        allowed_track_ids = {row["track_id"] for row in track_rows}
        artists = [row for row in artists if row["track_id"] in allowed_track_ids]

    return track_rows, artists


def init_duckdb(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("create schema if not exists raw")
    conn.execute("create schema if not exists analytics")

    conn.execute(
        """
        create table if not exists raw.spotify_saved_tracks (
            track_id varchar,
            added_at varchar,
            track_name varchar,
            isrc varchar,
            album_id varchar,
            album_name varchar,
            album_release_date varchar,
            album_total_tracks integer,
            track_number integer,
            disc_number integer,
            duration_ms integer,
            explicit boolean,
            popularity integer,
            is_local boolean,
            primary_artist_id varchar,
            primary_artist_name varchar,
            spotify_track_url varchar,
            ingest_run_at varchar
        )
        """
    )

    conn.execute("alter table raw.spotify_saved_tracks add column if not exists isrc varchar")

    conn.execute(
        """
        create table if not exists raw.spotify_track_artists (
            track_id varchar,
            artist_id varchar,
            artist_name varchar,
            artist_order integer,
            ingest_run_at varchar
        )
        """
    )


def upsert_spotify_data(
    conn: duckdb.DuckDBPyConnection,
    track_rows: List[Dict],
    artist_rows: List[Dict],
) -> None:
    if not track_rows:
        print("No saved tracks returned from Spotify.")
        return

    conn.execute(
        """
        create temp table incoming_tracks (
            track_id varchar,
            added_at varchar,
            track_name varchar,
            isrc varchar,
            album_id varchar,
            album_name varchar,
            album_release_date varchar,
            album_total_tracks integer,
            track_number integer,
            disc_number integer,
            duration_ms integer,
            explicit boolean,
            popularity integer,
            is_local boolean,
            primary_artist_id varchar,
            primary_artist_name varchar,
            spotify_track_url varchar,
            ingest_run_at varchar
        )
        """
    )

    conn.execute(
        """
        create temp table incoming_artists (
            track_id varchar,
            artist_id varchar,
            artist_name varchar,
            artist_order integer,
            ingest_run_at varchar
        )
        """
    )

    conn.executemany(
        """
        insert into incoming_tracks values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["track_id"],
                row["added_at"],
                row["track_name"],
                row["isrc"],
                row["album_id"],
                row["album_name"],
                row["album_release_date"],
                row["album_total_tracks"],
                row["track_number"],
                row["disc_number"],
                row["duration_ms"],
                row["explicit"],
                row["popularity"],
                row["is_local"],
                row["primary_artist_id"],
                row["primary_artist_name"],
                row["spotify_track_url"],
                row["ingest_run_at"],
            )
            for row in track_rows
        ],
    )

    if artist_rows:
        conn.executemany(
            """
            insert into incoming_artists values (?, ?, ?, ?, ?)
            """,
            [
                (
                    row["track_id"],
                    row["artist_id"],
                    row["artist_name"],
                    row["artist_order"],
                    row["ingest_run_at"],
                )
                for row in artist_rows
            ],
        )

    conn.execute("begin transaction")
    conn.execute(
        """
        delete from raw.spotify_track_artists
        where track_id in (select distinct track_id from incoming_tracks)
        """
    )
    conn.execute(
        """
        delete from raw.spotify_saved_tracks
        where track_id in (select distinct track_id from incoming_tracks)
        """
    )
    conn.execute(
        """
        insert into raw.spotify_saved_tracks (
            track_id,
            added_at,
            track_name,
            isrc,
            album_id,
            album_name,
            album_release_date,
            album_total_tracks,
            track_number,
            disc_number,
            duration_ms,
            explicit,
            popularity,
            is_local,
            primary_artist_id,
            primary_artist_name,
            spotify_track_url,
            ingest_run_at
        )
        select
            track_id,
            added_at,
            track_name,
            isrc,
            album_id,
            album_name,
            album_release_date,
            album_total_tracks,
            track_number,
            disc_number,
            duration_ms,
            explicit,
            popularity,
            is_local,
            primary_artist_id,
            primary_artist_name,
            spotify_track_url,
            ingest_run_at
        from incoming_tracks
        """
    )
    conn.execute(
        """
        insert into raw.spotify_track_artists (
            track_id,
            artist_id,
            artist_name,
            artist_order,
            ingest_run_at
        )
        select
            track_id,
            artist_id,
            artist_name,
            artist_order,
            ingest_run_at
        from incoming_artists
        """
    )
    conn.execute("commit")

    print(f"Upserted {len(track_rows)} tracks and {len(artist_rows)} track-artist rows.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest your Spotify saved tracks into DuckDB raw tables."
    )
    parser.add_argument(
        "--max-tracks",
        type=int,
        default=None,
        help="Optional cap for test ingestions.",
    )
    parser.add_argument(
        "--stop-before-added-at",
        type=str,
        default=None,
        help=(
            "Incremental cutoff: stop ingesting once Spotify returns rows with "
            "added_at earlier than this ISO-8601 timestamp."
        ),
    )
    args = parser.parse_args()

    load_dotenv()
    db_path = os.getenv("DUCKDB_PATH", "warehouse/music.duckdb")
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    client = get_spotify_client()
    cutoff_dt = parse_iso8601_utc(args.stop_before_added_at)
    track_rows, artist_rows = fetch_saved_tracks(
        client,
        max_tracks=args.max_tracks,
        stop_before_added_at=cutoff_dt,
    )

    conn = duckdb.connect(db_path)
    try:
        init_duckdb(conn)
        upsert_spotify_data(conn, track_rows, artist_rows)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
