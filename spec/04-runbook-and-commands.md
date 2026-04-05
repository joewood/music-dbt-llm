# 04. Runbook And Commands

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

## Environment variables

Required:

- `SPOTIPY_CLIENT_ID`
- `SPOTIPY_REDIRECT_URI`

Optional MusicBrainz user-agent identity:

- `MUSICBRAINZ_APP_NAME`
- `MUSICBRAINZ_APP_VERSION`
- `MUSICBRAINZ_CONTACT`

Optional:

- `DUCKDB_PATH` (default `warehouse/music.duckdb`)

## Spotify ingest

```powershell
python scripts/ingest_spotify_library.py
```

Sample:

```powershell
python scripts/ingest_spotify_library.py --max-tracks 200
```

## Decoupled MusicBrainz enrichment flow

1. Build queue relation:

```powershell
dbt run --select stg_musicbrainz_enrichment_queue
```

2. Export queue CSV:

```powershell
dbt run-operation export_musicbrainz_enrichment_queue --args '{output_path: exports/musicbrainz/enrichment_queue.csv, max_unenriched: 1000}'
```

3. Enrich queue CSV:

```powershell
python scripts/enrich_musicbrainz.py --input-csv exports/musicbrainz/enrichment_queue.csv --output-dir exports/musicbrainz/results --max-unenriched 1000
```

4. Load enrichment CSV outputs into DuckDB:

```powershell
dbt run-operation load_musicbrainz_enrichment_results --args '{input_dir: exports/musicbrainz/results}'
```

5. Build downstream models/tests:

```powershell
dbt run
dbt test
```

## One-command pipeline

```powershell
python scripts/run_full_pipeline.py --max-tracks 300 --max-unenriched 300
```

## VS Code tasks

Available in `.vscode/tasks.json`:

- `dbt: debug`
- `dbt: run`
- `dbt: test`
- `dbt: run + test`
