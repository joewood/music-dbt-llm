import argparse
import csv
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import musicbrainzngs
from dotenv import load_dotenv


ISRC_INCLUDES = ["artists", "releases"]
RECORDING_METADATA_INCLUDES = ["tags", "artist-rels", "work-rels", "recording-rels"]
WORK_METADATA_INCLUDES = ["artist-rels"]


def read_enrichment_queue(input_csv: Path, max_unenriched: int) -> Dict[str, List[str]]:
    if not input_csv.exists():
        raise FileNotFoundError(
            f"Queue CSV not found: {input_csv}. Run dbt queue export first."
        )

    mapping: Dict[str, List[str]] = {}
    with input_csv.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            track_id = (row.get("track_id") or "").strip()
            isrc = (row.get("isrc") or "").strip().upper()
            if not track_id or not isrc:
                continue
            mapping.setdefault(isrc, []).append(track_id)

    if max_unenriched > 0 and len(mapping) > max_unenriched:
        limited_isrcs = list(mapping.keys())[:max_unenriched]
        mapping = {isrc: mapping[isrc] for isrc in limited_isrcs}

    return mapping


def parse_recordings(payload: dict) -> List[dict]:
    return (payload.get("isrc") or {}).get("recording-list") or []


def _extract_work_links(recording: dict) -> List[str]:
    work_mbids: List[str] = []
    relation_lists = recording.get("relation-list") or []
    for rel_block in relation_lists:
        if (rel_block or {}).get("target-type") != "work":
            continue
        for rel in (rel_block or {}).get("relation") or []:
            work_obj = (rel or {}).get("work") or {}
            work_mbid = work_obj.get("id")
            if work_mbid:
                work_mbids.append(work_mbid)
    return work_mbids


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


def fetch_recording_metadata_payload(
    recording_mbid: str,
    fallback_recording: dict,
    sleep_seconds: float,
) -> Tuple[dict, bool]:
    try:
        detail_response = musicbrainzngs.get_recording_by_id(
            recording_mbid,
            includes=RECORDING_METADATA_INCLUDES,
        )
        # Respect MusicBrainz rate limits between metadata detail requests.
        time.sleep(sleep_seconds)
        return detail_response.get("recording") or fallback_recording, True
    except musicbrainzngs.WebServiceError as exc:
        print(f"Metadata lookup failed for recording {recording_mbid}: {exc}")
        return fallback_recording, False


def write_csv(path: Path, fieldnames: List[str], rows: Iterable[dict]) -> int:
    row_list = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(row_list)
    return len(row_list)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich queue CSV with MusicBrainz and emit CSV outputs for loading."
    )
    parser.add_argument(
        "--input-csv",
        default="exports/musicbrainz/enrichment_queue.csv",
        help="CSV exported by dbt with track_id and isrc columns.",
    )
    parser.add_argument(
        "--output-dir",
        default="exports/musicbrainz/results",
        help="Directory for enrichment result CSV files.",
    )
    parser.add_argument(
        "--max-unenriched",
        type=int,
        default=1000,
        help="Max distinct ISRC values to process from the queue CSV.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=1.1,
        help="Delay between MusicBrainz requests to respect rate limits.",
    )
    args = parser.parse_args()

    load_dotenv()

    app_name = os.getenv("MUSICBRAINZ_APP_NAME", "music-dbt")
    app_version = os.getenv("MUSICBRAINZ_APP_VERSION", "0.1.0")
    contact = os.getenv("MUSICBRAINZ_CONTACT", "noreply@example.com")
    musicbrainzngs.set_useragent(app_name, app_version, contact)

    input_csv = Path(args.input_csv)
    output_dir = Path(args.output_dir)

    isrc_to_tracks = read_enrichment_queue(input_csv, args.max_unenriched)
    if not isrc_to_tracks:
        print("No queued ISRC values found. Nothing to enrich.")
        return

    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    all_isrcs = list(isrc_to_tracks.keys())

    candidate_rows: List[dict] = []
    metadata_payload_cache: Dict[str, dict] = {}
    work_payload_cache: Dict[str, dict] = {}
    recording_payload_rows: Dict[str, dict] = {}
    work_payload_rows: Dict[str, dict] = {}

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
                payload, did_fetch_details = fetch_recording_metadata_payload(
                    recording_mbid,
                    recording,
                    args.sleep_seconds,
                )
                metadata_payload_cache[recording_mbid] = payload
                if did_fetch_details:
                    recording_payload_rows[recording_mbid] = {
                        "recording_mbid": recording_mbid,
                        "payload_json": json.dumps(payload),
                        "last_seen_at": now_iso,
                    }

            for work_mbid in _extract_work_links(metadata_payload_cache[recording_mbid]):
                if not work_mbid:
                    continue
                work_payload = fetch_work_payload(
                    work_mbid,
                    work_payload_cache,
                    args.sleep_seconds,
                )
                if work_payload:
                    work_payload_rows[work_mbid] = {
                        "work_mbid": work_mbid,
                        "payload_json": json.dumps(work_payload),
                        "last_seen_at": now_iso,
                    }

            for track_id in isrc_to_tracks[isrc]:
                candidate_rows.append(
                    {
                        "track_id": track_id,
                        "isrc": isrc,
                        "recording_mbid": recording_mbid,
                        "match_rank": rank,
                        "matched_at": now_iso,
                        "isrc_response_json": json.dumps(recording),
                    }
                )

        print(
            f"[{idx}/{len(all_isrcs)}] Processed ISRC {isrc} ({len(recordings)} candidates)"
        )
        time.sleep(args.sleep_seconds)

    # Deduplicate by (track_id, recording_mbid, match_rank) for stable outputs.
    deduped_candidates: Dict[Tuple[str, str, int], dict] = {}
    for row in candidate_rows:
        deduped_candidates[(row["track_id"], row["recording_mbid"], row["match_rank"])] = row

    candidates_csv = output_dir / "musicbrainz_isrc_candidates.csv"
    recording_payloads_csv = output_dir / "musicbrainz_recording_payloads.csv"
    work_payloads_csv = output_dir / "musicbrainz_work_payloads.csv"

    candidate_count = write_csv(
        candidates_csv,
        [
            "track_id",
            "isrc",
            "recording_mbid",
            "match_rank",
            "matched_at",
            "isrc_response_json",
        ],
        deduped_candidates.values(),
    )
    recording_payload_count = write_csv(
        recording_payloads_csv,
        ["recording_mbid", "payload_json", "last_seen_at"],
        recording_payload_rows.values(),
    )
    work_payload_count = write_csv(
        work_payloads_csv,
        ["work_mbid", "payload_json", "last_seen_at"],
        work_payload_rows.values(),
    )

    summary_path = output_dir / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "input_csv": str(input_csv),
                "processed_isrcs": len(all_isrcs),
                "rows": {
                    "musicbrainz_isrc_candidates": candidate_count,
                    "musicbrainz_recording_payloads": recording_payload_count,
                    "musicbrainz_work_payloads": work_payload_count,
                },
                "generated_at": now_iso,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        "Wrote CSV outputs: "
        f"{candidate_count} candidate rows, "
        f"{recording_payload_count} recording payload rows, "
        f"{work_payload_count} work payload rows."
    )


if __name__ == "__main__":
    main()
