import argparse
import datetime as dt
import json
import os
import time
from typing import Dict, List

import duckdb
import musicbrainzngs
from dotenv import load_dotenv


DEFAULT_DB_PATH = "warehouse/music.duckdb"
DBT_ENRICHMENT_QUEUE_RELATION = "analytics.stg_musicbrainz_enrichment_queue"
ISRC_INCLUDES = ["artists", "releases"]
RECORDING_METADATA_INCLUDES = ["tags", "artist-rels", "work-rels", "recording-rels"]
WORK_METADATA_INCLUDES = ["artist-rels"]

GENRE_KEYWORDS = {
    "alternative",
    "ambient",
    "blues",
    "classical",
    "country",
    "dance",
    "disco",
    "drum and bass",
    "dub",
    "dubstep",
    "edm",
    "electronic",
    "emo",
    "folk",
    "funk",
    "grime",
    "hip hop",
    "house",
    "indie",
    "industrial",
    "jazz",
    "k-pop",
    "latin",
    "metal",
    "new wave",
    "pop",
    "post-punk",
    "progressive",
    "punk",
    "r&b",
    "rap",
    "reggae",
    "rock",
    "soul",
    "synthpop",
    "techno",
    "trance",
    "trap",
}

GENRE_SUFFIXES = (
    "core",
    "house",
    "step",
    "wave",
)


def init_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("create schema if not exists raw")
    conn.execute(
        """
        create table if not exists raw.musicbrainz_recordings (
            recording_mbid varchar,
            title varchar,
            length_ms integer,
            primary_artist_name varchar,
            artist_credit_text varchar,
            release_title varchar,
            release_date varchar,
            disambiguation varchar,
            last_seen_at varchar
        )
        """
    )
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
    conn.execute(
        """
        create table if not exists raw.spotify_musicbrainz_map (
            track_id varchar,
            isrc varchar,
            recording_mbid varchar,
            match_source varchar,
            match_rank integer,
            matched_at varchar
        )
        """
    )
    conn.execute(
        """
        create table if not exists raw.musicbrainz_recording_metadata (
            recording_mbid varchar,
            top_genre varchar,
            top_style varchar,
            genre_list_json varchar,
            style_list_json varchar,
            instrument_list_json varchar,
            primary_work_mbid varchar,
            primary_work_title varchar,
            work_writer_list_json varchar,
            is_cover boolean,
            cover_of_recording_mbid varchar,
            cover_of_recording_title varchar,
            covered_by_recordings_json varchar,
            metadata_source varchar,
            last_seen_at varchar
        )
        """
    )
    conn.execute("alter table raw.musicbrainz_recording_metadata add column if not exists primary_work_mbid varchar")
    conn.execute("alter table raw.musicbrainz_recording_metadata add column if not exists primary_work_title varchar")
    conn.execute("alter table raw.musicbrainz_recording_metadata add column if not exists work_writer_list_json varchar")
    conn.execute("alter table raw.musicbrainz_recording_metadata add column if not exists is_cover boolean")
    conn.execute("alter table raw.musicbrainz_recording_metadata add column if not exists cover_of_recording_mbid varchar")
    conn.execute("alter table raw.musicbrainz_recording_metadata add column if not exists cover_of_recording_title varchar")
    conn.execute("alter table raw.musicbrainz_recording_metadata add column if not exists covered_by_recordings_json varchar")
    conn.execute(
        """
        create table if not exists raw.musicbrainz_artists (
            artist_mbid varchar,
            artist_name varchar,
            sort_name varchar,
            disambiguation varchar,
            artist_type varchar,
            artist_country varchar,
            artist_gender varchar,
            credit_order integer,
            source_recording_mbid varchar,
            last_seen_at varchar
        )
        """
    )
    conn.execute(
        """
        create table if not exists raw.musicbrainz_releases (
            release_mbid varchar,
            release_title varchar,
            release_date varchar,
            release_country varchar,
            release_status varchar,
            release_group_mbid varchar,
            release_group_title varchar,
            release_group_primary_type varchar,
            release_group_secondary_types_json varchar,
            source_recording_mbid varchar,
            last_seen_at varchar
        )
        """
    )
    conn.execute(
        """
        create table if not exists raw.musicbrainz_works (
            work_mbid varchar,
            work_title varchar,
            work_type varchar,
            work_language varchar,
            disambiguation varchar,
            last_seen_at varchar
        )
        """
    )
    conn.execute(
        """
        create table if not exists raw.musicbrainz_work_writers (
            work_mbid varchar,
            artist_mbid varchar,
            artist_name varchar,
            writer_role varchar,
            attributes_json varchar,
            last_seen_at varchar
        )
        """
    )


def get_tracks_needing_enrichment(
    conn: duckdb.DuckDBPyConnection,
    max_unenriched: int,
) -> Dict[str, List[str]]:
    rows = _read_tracks_from_dbt_queue(conn, max_unenriched)
    return _rows_to_isrc_mapping(rows)


def _rows_to_isrc_mapping(rows: List[tuple]) -> Dict[str, List[str]]:
    mapping: Dict[str, List[str]] = {}
    for track_id, isrc in rows:
        mapping.setdefault(isrc, []).append(track_id)

    return mapping


def _relation_exists(conn: duckdb.DuckDBPyConnection, relation: str) -> bool:
    schema_name, table_name = relation.split(".", maxsplit=1)
    result = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = ?
          and table_name = ?
        """,
        [schema_name, table_name],
    ).fetchone()
    return bool(result and result[0] > 0)


def _read_tracks_from_dbt_queue(
    conn: duckdb.DuckDBPyConnection,
    max_unenriched: int,
) -> List[tuple]:
    if not _relation_exists(conn, DBT_ENRICHMENT_QUEUE_RELATION):
        raise RuntimeError(
            "Missing dbt enrichment queue relation "
            f"'{DBT_ENRICHMENT_QUEUE_RELATION}'. Run `dbt run --select stg_musicbrainz_enrichment_queue` "
            "before running enrichment."
        )

    print(f"Using dbt enrichment queue from {DBT_ENRICHMENT_QUEUE_RELATION}.")
    return conn.execute(
        f"""
        select
            track_id,
            isrc
        from {DBT_ENRICHMENT_QUEUE_RELATION}
        where isrc_queue_rank <= ?
        order by isrc, track_id
        """,
        [max_unenriched],
    ).fetchall()


def parse_recordings(payload: dict) -> List[dict]:
    return (payload.get("isrc") or {}).get("recording-list") or []


def get_artist_credit_text(recording: dict) -> str | None:
    artist_credits = recording.get("artist-credit") or []
    credit_names = [
        item.get("artist", {}).get("name")
        for item in artist_credits
        if isinstance(item, dict)
    ]
    credit_names = [name for name in credit_names if name]
    if not credit_names:
        return None
    return ", ".join(credit_names)


def _ranked_name_list(items: list[dict], key: str = "name") -> List[str]:
    ranked = []
    for item in items or []:
        name = (item or {}).get(key)
        if not name:
            continue
        count_val = (item or {}).get("count")
        try:
            count_num = int(count_val) if count_val is not None else 0
        except ValueError:
            count_num = 0
        ranked.append((name, count_num))
    ranked.sort(key=lambda x: (-x[1], x[0].lower()))
    return [name for name, _ in ranked]


def _extract_instruments(recording: dict) -> List[str]:
    instruments: set[str] = set()
    relation_lists = recording.get("relation-list") or []
    for rel_block in relation_lists:
        if (rel_block or {}).get("target-type") != "artist":
            continue
        for rel in (rel_block or {}).get("relation") or []:
            for attr in (rel or {}).get("attribute-list") or []:
                value = (attr or "").strip()
                if not value:
                    continue
                instruments.add(value)
    return sorted(instruments)


def _normalize_tag(tag: str) -> str:
    return " ".join((tag or "").strip().lower().replace("-", " ").split())


def _is_genre_tag(tag: str) -> bool:
    normalized = _normalize_tag(tag)
    if not normalized:
        return False

    if normalized in GENRE_KEYWORDS:
        return True

    parts = normalized.split()
    if any(part in GENRE_KEYWORDS for part in parts):
        return True

    if any(normalized.endswith(suffix) for suffix in GENRE_SUFFIXES):
        return True

    return False


def split_tags_into_genre_and_style(tags: List[str]) -> tuple[List[str], List[str]]:
    genres: List[str] = []
    styles: List[str] = []
    seen_genres: set[str] = set()
    seen_styles: set[str] = set()

    for tag in tags:
        key = _normalize_tag(tag)
        if not key:
            continue
        if _is_genre_tag(tag):
            if key not in seen_genres:
                genres.append(tag)
                seen_genres.add(key)
        else:
            if key not in seen_styles:
                styles.append(tag)
                seen_styles.add(key)

    return genres, styles


def extract_recording_metadata(recording: dict) -> tuple[str | None, str | None, str, str, str]:
    genre_list = _ranked_name_list(recording.get("genre-list") or [])
    tag_list = _ranked_name_list(recording.get("tag-list") or [])

    # Older MB clients may not expose genre-list for this query path. In that
    # case, infer genre/style from ranked tags.
    if not genre_list:
        genre_list, style_list = split_tags_into_genre_and_style(tag_list)
    else:
        genre_names_lower = {g.lower() for g in genre_list}
        style_list = [tag for tag in tag_list if tag.lower() not in genre_names_lower]

    instrument_list = _extract_instruments(recording)

    top_genre = genre_list[0] if genre_list else None
    top_style = style_list[0] if style_list else None
    return (
        top_genre,
        top_style,
        json.dumps(genre_list),
        json.dumps(style_list),
        json.dumps(instrument_list),
    )


def _extract_work_links(recording: dict) -> List[dict]:
    links: List[dict] = []
    relation_lists = recording.get("relation-list") or []
    for rel_block in relation_lists:
        if (rel_block or {}).get("target-type") != "work":
            continue
        for rel in (rel_block or {}).get("relation") or []:
            work_obj = (rel or {}).get("work") or {}
            work_mbid = work_obj.get("id")
            work_title = work_obj.get("title") or work_obj.get("name")
            if work_mbid:
                links.append({"work_mbid": work_mbid, "work_title": work_title})
    return links


def _extract_work_writers(work_payload: dict, work_mbid: str, now_iso: str) -> tuple[List[tuple], List[str]]:
    writer_rows: List[tuple] = []
    writer_names: set[str] = set()
    relation_lists = work_payload.get("relation-list") or []
    for rel_block in relation_lists:
        if (rel_block or {}).get("target-type") != "artist":
            continue
        for rel in (rel_block or {}).get("relation") or []:
            rel_type = ((rel or {}).get("type") or "").strip().lower()
            if rel_type not in {"writer", "composer", "lyricist", "librettist"}:
                continue
            artist_obj = (rel or {}).get("artist") or {}
            artist_mbid = artist_obj.get("id")
            artist_name = artist_obj.get("name")
            if artist_name:
                writer_names.add(artist_name)
            if artist_mbid:
                writer_rows.append(
                    (
                        work_mbid,
                        artist_mbid,
                        artist_name,
                        rel_type,
                        json.dumps((rel or {}).get("attribute-list") or []),
                        now_iso,
                    )
                )
    return writer_rows, sorted(writer_names)


def fetch_work_payload(
    work_mbid: str,
    work_payload_cache: Dict[str, dict],
    sleep_seconds: float,
) -> dict:
    if work_mbid in work_payload_cache:
        return work_payload_cache[work_mbid]

    try:
        response = musicbrainzngs.get_work_by_id(work_mbid, includes=WORK_METADATA_INCLUDES)
        time.sleep(sleep_seconds)
        work_payload = response.get("work") or {}
        work_payload_cache[work_mbid] = work_payload
        return work_payload
    except musicbrainzngs.WebServiceError as exc:
        print(f"Work metadata lookup failed for {work_mbid}: {exc}")
        work_payload_cache[work_mbid] = {}
        return {}


def _extract_artists_from_recording(recording: dict, source_recording_mbid: str, now_iso: str) -> List[tuple]:
    rows: List[tuple] = []
    artist_credit = recording.get("artist-credit") or []
    for idx, item in enumerate(artist_credit, start=1):
        if not isinstance(item, dict):
            continue
        artist = item.get("artist") or {}
        artist_mbid = artist.get("id")
        if not artist_mbid:
            continue
        rows.append(
            (
                artist_mbid,
                artist.get("name"),
                artist.get("sort-name"),
                artist.get("disambiguation"),
                artist.get("type"),
                artist.get("country"),
                artist.get("gender"),
                idx,
                source_recording_mbid,
                now_iso,
            )
        )
    return rows


def _extract_releases_from_recording(recording: dict, source_recording_mbid: str, now_iso: str) -> List[tuple]:
    rows: List[tuple] = []
    for release in recording.get("release-list") or []:
        release_mbid = (release or {}).get("id")
        if not release_mbid:
            continue
        release_group = (release or {}).get("release-group") or {}
        rows.append(
            (
                release_mbid,
                (release or {}).get("title"),
                (release or {}).get("date"),
                (release or {}).get("country"),
                (release or {}).get("status"),
                release_group.get("id"),
                release_group.get("title"),
                release_group.get("primary-type") or release_group.get("type"),
                json.dumps(release_group.get("secondary-type-list") or []),
                source_recording_mbid,
                now_iso,
            )
        )
    return rows


def extract_cover_metadata(recording: dict) -> tuple[bool, str | None, str | None, str]:
    is_cover = False
    cover_of_recording_mbid = None
    cover_of_recording_title = None
    covered_by: List[dict] = []

    relation_lists = recording.get("relation-list") or []
    for rel_block in relation_lists:
        if (rel_block or {}).get("target-type") != "recording":
            continue
        for rel in (rel_block or {}).get("relation") or []:
            rel_type = ((rel or {}).get("type") or "").strip().lower()
            target_recording = (rel or {}).get("recording") or {}
            target_mbid = target_recording.get("id")
            target_title = target_recording.get("title") or target_recording.get("name")

            if rel_type == "cover of":
                is_cover = True
                cover_of_recording_mbid = target_mbid
                cover_of_recording_title = target_title
            elif rel_type == "covers" and target_mbid:
                covered_by.append({"recording_mbid": target_mbid, "title": target_title})

    return (
        is_cover,
        cover_of_recording_mbid,
        cover_of_recording_title,
        json.dumps(covered_by),
    )


def fetch_recording_metadata_payload(
    recording_mbid: str,
    fallback_recording: dict,
    sleep_seconds: float,
) -> tuple[dict, bool, str | None]:
    try:
        detail_response = musicbrainzngs.get_recording_by_id(
            recording_mbid,
            includes=RECORDING_METADATA_INCLUDES,
        )
        # Respect MusicBrainz rate limits between metadata detail requests.
        time.sleep(sleep_seconds)
        return detail_response.get("recording") or fallback_recording, True, None
    except musicbrainzngs.WebServiceError as exc:
        print(f"Metadata lookup failed for recording {recording_mbid}: {exc}")
        return fallback_recording, False, str(exc)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich Spotify tracks with MusicBrainz recording references via ISRC."
    )
    parser.add_argument(
        "--max-unenriched",
        type=int,
        default=1000,
        help="Max distinct unenriched ISRC values to process per run.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.1,
        help="Delay between MusicBrainz requests to respect rate limits.",
    )
    args = parser.parse_args()

    load_dotenv()

    db_path = os.getenv("DUCKDB_PATH", DEFAULT_DB_PATH)
    db_dir = os.path.dirname(db_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    app_name = os.getenv("MUSICBRAINZ_APP_NAME", "music-dbt")
    app_version = os.getenv("MUSICBRAINZ_APP_VERSION", "0.1.0")
    contact = os.getenv("MUSICBRAINZ_CONTACT", "noreply@example.com")
    musicbrainzngs.set_useragent(app_name, app_version, contact)

    conn = duckdb.connect(db_path)
    try:
        init_tables(conn)
        isrc_to_tracks = get_tracks_needing_enrichment(conn, args.max_unenriched)
        if not isrc_to_tracks:
            print("No tracks missing mappings or metadata. Nothing to enrich.")
            return

        now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
        all_isrcs = list(isrc_to_tracks.keys())

        candidate_rows: List[tuple] = []
        metadata_payload_cache: Dict[str, dict] = {}
        work_payload_cache: Dict[str, dict] = {}
        recording_payload_rows: List[tuple] = []
        work_payload_rows: List[tuple] = []

        for idx, isrc in enumerate(all_isrcs, start=1):
            try:
                response = musicbrainzngs.get_recordings_by_isrc(
                    isrc,
                    includes=ISRC_INCLUDES,
                )
            except musicbrainzngs.WebServiceError as exc:
                print(f"[{idx}/{len(all_isrcs)}] ISRC {isrc} failed: {exc}")
                time.sleep(args.sleep_seconds)
                continue

            recordings = parse_recordings(response)
            for rank, recording in enumerate(recordings, start=1):
                recording_mbid = recording.get("id")
                if not recording_mbid:
                    continue

                if recording_mbid not in metadata_payload_cache:
                    payload, did_fetch_details, _ = fetch_recording_metadata_payload(
                        recording_mbid,
                        recording,
                        args.sleep_seconds,
                    )
                    metadata_payload_cache[recording_mbid] = payload
                    if did_fetch_details:
                        recording_payload_rows.append(
                            (
                                recording_mbid,
                                json.dumps(metadata_payload_cache[recording_mbid]),
                                now_iso,
                            )
                        )

                work_links = _extract_work_links(metadata_payload_cache[recording_mbid])

                for work_link in work_links:
                    work_mbid = work_link.get("work_mbid")
                    if not work_mbid:
                        continue

                    work_payload = fetch_work_payload(
                        work_mbid,
                        work_payload_cache,
                        args.sleep_seconds,
                    )
                    if work_payload:
                        work_payload_rows.append(
                            (
                                work_mbid,
                                json.dumps(work_payload),
                                now_iso,
                            )
                        )

                for track_id in isrc_to_tracks[isrc]:
                    candidate_rows.append(
                        (
                            track_id,
                            isrc,
                            recording_mbid,
                            rank,
                            now_iso,
                            json.dumps(recording),
                        )
                    )

            print(
                f"[{idx}/{len(all_isrcs)}] Processed ISRC {isrc} ({len(recordings)} candidates)"
            )
            time.sleep(args.sleep_seconds)

        if candidate_rows:
            deduped_candidates = {}
            for row in candidate_rows:
                deduped_candidates[(row[0], row[2], row[3])] = row
            candidate_rows = list(deduped_candidates.values())
            conn.executemany(
                "delete from raw.musicbrainz_isrc_candidates where track_id = ?",
                sorted({(row[0],) for row in candidate_rows}),
            )
            conn.executemany(
                """
                insert into raw.musicbrainz_isrc_candidates values (?, ?, ?, ?, ?, ?)
                """,
                candidate_rows,
            )

        if recording_payload_rows:
            deduped_payloads = {}
            for row in recording_payload_rows:
                deduped_payloads[row[0]] = row
            recording_payload_rows = list(deduped_payloads.values())
            conn.executemany(
                "delete from raw.musicbrainz_recording_payloads where recording_mbid = ?",
                [(row[0],) for row in recording_payload_rows],
            )
            conn.executemany(
                """
                insert into raw.musicbrainz_recording_payloads values (?, ?, ?)
                """,
                recording_payload_rows,
            )

        if work_payload_rows:
            deduped_work_payloads = {}
            for row in work_payload_rows:
                deduped_work_payloads[row[0]] = row
            work_payload_rows = list(deduped_work_payloads.values())
            conn.executemany(
                "delete from raw.musicbrainz_work_payloads where work_mbid = ?",
                [(row[0],) for row in work_payload_rows],
            )
            conn.executemany(
                """
                insert into raw.musicbrainz_work_payloads values (?, ?, ?)
                """,
                work_payload_rows,
            )

        if candidate_rows or recording_payload_rows or work_payload_rows:
            print(
                "Inserted "
                f"{len(candidate_rows)} ISRC candidate rows, "
                f"{len(recording_payload_rows)} recording payload rows, "
                f"{len(work_payload_rows)} work payload rows."
            )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
