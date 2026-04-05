# 02. Architecture And Flow

## High-level architecture

1. `scripts/ingest_spotify_library.py`
   - Authenticates with Spotify.
   - Pulls saved tracks.
   - Writes to DuckDB raw tables.
2. dbt staging queue model
   - `models/staging/stg_musicbrainz_enrichment_queue.sql`
   - Identifies tracks/ISRCs requiring enrichment.
3. dbt CSV export macro
   - `macros/export_musicbrainz_enrichment_queue.sql`
   - Exports enrichment queue to CSV.
4. CSV-only MusicBrainz fetcher
   - `scripts/enrich_musicbrainz.py`
   - Reads queue CSV.
   - Calls MusicBrainz APIs.
   - Writes output CSV files.
5. dbt CSV-to-DuckDB loader operation
   - `macros/load_musicbrainz_enrichment_results.sql`
   - `dbt run-operation load_musicbrainz_enrichment_results`
   - Imports enrichment output CSV files into DuckDB raw tables.
6. dbt staging + marts
   - Build playlist-ready and entity-aware models.

## Separation of concerns (final agreed design)

- dbt owns selection logic for what needs enrichment.
- Enrichment script owns external API retrieval and output file generation.
- dbt run-operation macro owns persistence of fetched results into DuckDB.
- This removes direct DB dependency from the enrichment fetch process.

## Pipeline execution order

1. Spotify ingest.
2. dbt build of enrichment queue relation.
3. dbt export of queue CSV.
4. MusicBrainz CSV enrichment.
5. MusicBrainz CSV load into DuckDB raw tables.
6. dbt run + test for downstream models.


## Supporting components

- `scripts/spotify_auth.py`: shared Spotify PKCE callback auth helper.
- `macros/init_raw_tables.sql`: creates required raw tables when invoked via `dbt run-operation init_raw_spotify_tables`.
- `scripts/run_full_pipeline.py`: orchestrates full process.
