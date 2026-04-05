# music-dbt

A dbt + DuckDB project that ingests your Spotify saved library and prepares playlist-ready models.

Implementation note: Python application logic now lives under `src/music_dbt`.
Files under `scripts/` are thin CLI wrappers kept for stable command paths.

## Architecture at a glance

- Bronze (`raw.*`): ingestion and enrichment landing tables populated by Python/dbt operations.
- Silver staging (`models/staging/stg_*`): source-conformed models; only staging models query `source()`.
- Silver enrichment/intermediate (`models/refinement/int_*`): joins and enrichment rollups built from staging models.
- Gold marts (`models/marts/fct_*`, `models/marts/dim_*`): final use-case projections for analytics and playlist workflows.

Pipeline boundary for MusicBrainz enrichment:

- Enrichment feeder model: `stg_musicbrainz_enrichment_queue` (tag: `enrichment_feed`)
- Post-enrichment models: tagged `post_enrichment`

## What this project builds

- `raw.spotify_saved_tracks`: saved tracks from your Spotify library.
- `raw.spotify_track_artists`: one row per artist per saved track.
- `raw.spotify_musicbrainz_map`: crosswalk from Spotify tracks to MusicBrainz recordings.
- `raw.musicbrainz_recordings`: MusicBrainz recording reference data.
- `analytics.stg_spotify_saved_tracks` (dbt staging view): cleaned track fields.
- `analytics.stg_spotify_musicbrainz_map` (dbt staging view): best MB match per track.
- `analytics.stg_musicbrainz_recordings` (dbt staging view): latest MB recording snapshot.
- `analytics.fct_playlist_ready_tracks` (dbt mart table): denormalized artist names and a `playlist_fit_score`.
- `analytics.fct_artist_stats` (dbt mart table): artist-level stats across your saved tracks.

## Naming conventions

- Staging models: `stg_<system>_<entity>`
- Intermediate/enrichment models: `int_<domain>_<purpose>_base`
- Fact marts: `fct_<subject>`
- Dimension marts: `dim_<subject>`
- Analyses: snake_case query names under `analyses/`

## Prerequisites

- Python 3.10+
- Spotify developer app credentials
- A Spotify Premium account is not required for library ingestion

## Spotify app setup

1. Go to [Spotify Developer Dashboard](https://developer.spotify.com/dashboard).
2. Create an app.
3. In app settings, add this Redirect URI (or your own): `http://127.0.0.1:8888/callback`
4. Keep your Client ID.

## Local setup (PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env` and fill in:

- `SPOTIPY_CLIENT_ID`
- `SPOTIPY_REDIRECT_URI`

Optional MusicBrainz user-agent values (recommended):

- `MUSICBRAINZ_APP_NAME=music-dbt`
- `MUSICBRAINZ_APP_VERSION=0.1.0`
- `MUSICBRAINZ_CONTACT=you@example.com`

## Ingest Spotify library into DuckDB

```powershell
python scripts/ingest_spotify_library.py
```

On first run, a browser opens for Spotify OAuth consent.

Optional test run with a small sample:

```powershell
python scripts/ingest_spotify_library.py --max-tracks 200
```

## Enrich with MusicBrainz reference data

The enrichment flow is now decoupled from DuckDB:

1. dbt builds the enrichment queue table.
2. dbt exports the queue as CSV.
3. `enrich_musicbrainz.py` reads only CSV and writes CSV outputs.
4. `dbt run-operation load_musicbrainz_enrichment_results` loads those outputs into raw DuckDB tables.

Run the steps manually:

```powershell
& $env:DBT_CMD run --select stg_musicbrainz_enrichment_queue
& $env:DBT_CMD run-operation export_musicbrainz_enrichment_queue --args '{output_path: exports/musicbrainz/enrichment_queue.csv, max_unenriched: 1000}'
python scripts/enrich_musicbrainz.py --input-csv exports/musicbrainz/enrichment_queue.csv --output-dir exports/musicbrainz/results --max-unenriched 1000
& $env:DBT_CMD run-operation load_musicbrainz_enrichment_results --args '{input_dir: exports/musicbrainz/results}'
```

The enrichment script only calls the API for currently queued tracks and processes
up to 1000 distinct ISRC values per run by default.

API fallback enrichment also captures recording metadata into
`raw.musicbrainz_recording_metadata` (genres, style-like tags, and instrument
hints parsed from performer relations), plus work/copyright links such as
related work MBIDs, writer lists, and cover/original relationships when
available.

Additional MusicBrainz entity tables are also populated during enrichment:

- `raw.musicbrainz_artists`
- `raw.musicbrainz_releases` (including release-group type such as Album/Single)
- `raw.musicbrainz_works`
- `raw.musicbrainz_work_writers`

Optional sampled enrichment:

```powershell
& $env:DBT_CMD run-operation export_musicbrainz_enrichment_queue --args '{output_path: exports/musicbrainz/enrichment_queue.csv, max_unenriched: 250}'
python scripts/enrich_musicbrainz.py --input-csv exports/musicbrainz/enrichment_queue.csv --output-dir exports/musicbrainz/results --max-unenriched 250
& $env:DBT_CMD run-operation load_musicbrainz_enrichment_results --args '{input_dir: exports/musicbrainz/results}'
```

CSV outputs produced by enrichment:

- `exports/musicbrainz/results/musicbrainz_isrc_candidates.csv`
- `exports/musicbrainz/results/musicbrainz_recording_payloads.csv`
- `exports/musicbrainz/results/musicbrainz_work_payloads.csv`
- `exports/musicbrainz/results/summary.json`

## Run dbt models

Fusion note: this project now uses the dbt Fusion CLI (`dbt`/`dbtf`) instead of
the Python `dbt-core` package.

On Windows, pin an explicit Fusion command in your current shell to avoid
accidentally invoking a `dbt-core` executable from a virtualenv:

```powershell
$env:DBT_CMD = Join-Path $env:USERPROFILE '.local\\bin\\dbt.exe'
```

Initialize raw schema/tables once (or whenever you need to re-bootstrap):

```powershell
& $env:DBT_CMD run-operation init_raw_spotify_tables --profiles-dir profiles
```

```powershell
& $env:DBT_CMD debug --profiles-dir profiles
& $env:DBT_CMD run --profiles-dir profiles
& $env:DBT_CMD test --profiles-dir profiles
```

On Windows ARM64 (for example Snapdragon X), the official installer may fail with
"Only x64 architecture is supported". In that case, install Fusion manually:

```powershell
$ver = "2.0.0-preview.164"
$target = "x86_64-pc-windows-msvc"
$url = "https://public.cdn.getdbt.com/fs/cli/fs-v$ver-$target.zip"
$tmp = Join-Path $env:TEMP ("dbt-fusion-manual-" + [guid]::NewGuid().ToString())
New-Item -ItemType Directory -Path $tmp | Out-Null
Invoke-WebRequest -Uri $url -OutFile (Join-Path $tmp "fs.zip") -UseBasicParsing
Expand-Archive -Path (Join-Path $tmp "fs.zip") -DestinationPath $tmp -Force
$exe = Get-ChildItem -Path $tmp -Filter *.exe -Recurse | Select-Object -First 1
$dest = Join-Path $env:USERPROFILE ".local\\bin"
New-Item -ItemType Directory -Path $dest -Force | Out-Null
Copy-Item -Path $exe.FullName -Destination (Join-Path $dest "dbt.exe") -Force
& (Join-Path $dest "dbt.exe") --version
```

Or run ingestion + enrichment + dbt in one command:

```powershell
python scripts/run_full_pipeline.py
```

`run_full_pipeline.py` auto-detects Fusion and also honors `DBT_CMD`
(or `--dbt-cmd`) if you want to force a specific executable.

Optional sampled run:

```powershell
python scripts/run_full_pipeline.py --max-tracks 300 --max-unenriched 300
```

DBT-only operations are also available via a single run-operation:

```powershell
& $env:DBT_CMD run-operation run_musicbrainz_enrichment_ops --args '{output_path: exports/musicbrainz/enrichment_queue.csv, max_unenriched: 1000, input_dir: exports/musicbrainz/results, do_export: true, do_load: true}'
```

## Quality checks

Use these to validate project health end-to-end:

```powershell
& $env:DBT_CMD parse --profiles-dir profiles
& $env:DBT_CMD build --profiles-dir profiles
```

Optional targeted checks for enrichment boundary models:

```powershell
& $env:DBT_CMD run --profiles-dir profiles --select tag:enrichment_feed
& $env:DBT_CMD run --profiles-dir profiles --select tag:post_enrichment
```

## Useful queries



Top playlist candidates:

```sql
select
  track_name,
  artist_names,
  popularity,
  freshness_score,
  playlist_fit_score
from analytics.fct_playlist_ready_tracks
order by playlist_fit_score desc
limit 50;
```

Entity-rich auto-playlist candidates:

```sql
select
  track_name,
  artist_names,
  top_genre,
  top_style,
  writer_names,
  release_group_types,
  is_cover,
  auto_playlist_score
from analytics.fct_playlist_entity_context
order by auto_playlist_score desc
limit 50;
```

Most represented artists:

```sql
select *
from analytics.fct_artist_stats
limit 50;
```

## Suggested next step

Use `analyses/playlist_candidates.sql` as the first query in your playlist pipeline, then add additional ranking constraints (tempo, era spread, artist diversity caps) in new marts.
