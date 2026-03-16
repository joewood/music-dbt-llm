# 01. Project Scope

## Objective

Build a local analytics pipeline that:

1. Ingests Spotify saved tracks into DuckDB.
2. Transforms data with dbt into playlist-ready marts.
3. Enriches tracks with MusicBrainz reference metadata.
4. Supports iterative playlist-generation logic from marts.

## Functional requirements implemented

1. dbt project configured for DuckDB (`music_dbt` profile).
2. Spotify OAuth auth flow with local callback server (PKCE via Spotipy).
3. Spotify ingest script writes `raw.spotify_saved_tracks` and `raw.spotify_track_artists`.
4. ISRC captured from Spotify for cross-system matching.
5. MusicBrainz enrichment flow implemented and integrated.
6. Staging and mart models for playlist scoring and entity context.
7. One-command pipeline runner script.
8. VS Code tasks for dbt operations.

## Non-goals currently

1. Direct automated playlist creation in Spotify API.
2. Production orchestration/deployment (cloud scheduler, CI pipeline).
3. Full confidence scoring/match disambiguation beyond current rank-based handling.

## Environment assumptions

- Windows local development environment.
- Python virtual environment in `.venv/`.
- DuckDB local file storage under `warehouse/`.
- dbt extension in VS Code used for development convenience.
