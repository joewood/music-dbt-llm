# music-dbt

A dbt + DuckDB project that ingests your Spotify saved library and prepares playlist-ready models.

## What this project builds

- `raw.spotify_saved_tracks`: saved tracks from your Spotify library.
- `raw.spotify_track_artists`: one row per artist per saved track.
- `raw.spotify_musicbrainz_map`: crosswalk from Spotify tracks to MusicBrainz recordings.
- `raw.musicbrainz_recordings`: MusicBrainz recording reference data.
- `analytics.stg_spotify_saved_tracks` (dbt staging view): cleaned track fields.
- `analytics.stg_spotify_musicbrainz_map` (dbt staging view): best MB match per track.
- `analytics.stg_musicbrainz_recordings` (dbt staging view): latest MB recording snapshot.
- `analytics.mart_playlist_ready_tracks` (dbt mart table): denormalized artist names and a `playlist_fit_score`.
- `analytics.mart_artist_stats` (dbt mart table): artist-level stats across your saved tracks.

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

After Spotify ingestion populates ISRC values, run:

```powershell
python scripts/enrich_musicbrainz.py --max-unenriched 1000
```

The script only calls the API for currently unenriched tracks and processes up to
1000 distinct ISRC values per run by default.

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
python scripts/enrich_musicbrainz.py --max-unenriched 250

## Run dbt models

Fusion note: this project now uses the dbt Fusion CLI (`dbt`/`dbtf`) instead of
the Python `dbt-core` package.

```powershell
dbt debug --profiles-dir profiles
dbt run --profiles-dir profiles
dbt test --profiles-dir profiles
```

On Windows ARM64 (for example Snapdragon X), the official installer may fail with
"Only x64 architecture is supported". In that case, install Fusion manually:

```powershell
$ver = "2.0.0-preview.145"
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
.\scripts\run_full_pipeline.ps1
```

Optional sampled run:

```powershell
.\scripts\run_full_pipeline.ps1 -MaxTracks 300
```

Bash equivalent (Linux/macOS/Git Bash):

```bash
./scripts/run_full_pipeline.sh 300 1000
```

Arguments are:

- first: max Spotify tracks to ingest (`0` means no max)
- second: max unenriched ISRC values for MusicBrainz enrichment

## Useful queries

Top playlist candidates:

```sql
select
  track_name,
  artist_names,
  popularity,
  freshness_score,
  playlist_fit_score
from analytics.mart_playlist_ready_tracks
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
from analytics.mart_playlist_entity_context
order by auto_playlist_score desc
limit 50;
```

Most represented artists:

```sql
select *
from analytics.mart_artist_stats
limit 50;
```

## Suggested next step

Use `analyses/playlist_candidates.sql` as the first query in your playlist pipeline, then add additional ranking constraints (tempo, era spread, artist diversity caps) in new marts.
