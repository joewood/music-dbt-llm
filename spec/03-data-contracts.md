# 03. Data Contracts

## DuckDB raw tables

Spotify raw:

- `raw.spotify_saved_tracks`
  - includes `track_id`, `added_at`, `track_name`, `isrc`, artist and album metadata.
- `raw.spotify_track_artists`
  - one row per track artist with ordering.

MusicBrainz raw ingest outputs:

- `raw.musicbrainz_isrc_candidates`
  - link candidates from Spotify track/ISRC to MB recording with rank and payload snippet.
- `raw.musicbrainz_recording_payloads`
  - JSON payload snapshots for MB recordings.
- `raw.musicbrainz_work_payloads`
  - JSON payload snapshots for MB works.

Derived/enriched raw tables used by downstream models:

- `raw.musicbrainz_recording_metadata`
- `raw.musicbrainz_artists`
- `raw.musicbrainz_releases`
- `raw.musicbrainz_works`
- `raw.musicbrainz_work_writers`

## dbt source declarations

Defined in `models/sources.yml` for all raw tables above under source `spotify_raw`.

## dbt queue contract (dbt -> enrichment script)

Exported CSV path default:

- `exports/musicbrainz/enrichment_queue.csv`

Required columns:

- `track_id`
- `isrc`
- `isrc_queue_rank`
- `isrc_track_rank`

Producer:

- macro: `export_musicbrainz_enrichment_queue`
- relation: `analytics.stg_musicbrainz_enrichment_queue`

## enrichment output contract (enrichment script -> loader)

Output directory default:

- `exports/musicbrainz/results/`

Files:

- `musicbrainz_isrc_candidates.csv`
  - `track_id,isrc,recording_mbid,match_rank,matched_at,isrc_response_json`
- `musicbrainz_recording_payloads.csv`
  - `recording_mbid,payload_json,last_seen_at`
- `musicbrainz_work_payloads.csv`
  - `work_mbid,payload_json,last_seen_at`
- `summary.json`
  - run metadata and row counts

Consumer:

- `scripts/load_musicbrainz_csv_to_duckdb.py`

## dbt consumption (staging/marts)

Key staging models:

- `models/staging/stg_spotify_saved_tracks.sql`
- `models/staging/stg_spotify_musicbrainz_map.sql`
- `models/staging/stg_musicbrainz_recordings.sql`
- `models/staging/stg_musicbrainz_recording_metadata.sql`
- `models/staging/stg_musicbrainz_artists.sql`
- `models/staging/stg_musicbrainz_releases.sql`
- `models/staging/stg_musicbrainz_works.sql`
- `models/staging/stg_musicbrainz_work_writers.sql`

Key marts:

- `models/marts/fct_playlist_ready_tracks.sql`
- `models/marts/fct_playlist_entity_context.sql`
- `models/marts/fct_artist_stats.sql`
