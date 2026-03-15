param(
    [int]$MaxTracks = 1000
)

$ErrorActionPreference = 'Stop'


function Get-MaxAddedAt {
    try {
        $value = (python .\scripts\get_max_added_at.py 2>$null).Trim()
        Write-Host "Found Max: $value"
        return $value
    } catch {
        Write-Host "Error Max: $($_.Exception.Message). Defaulting to full ingestion."
        return ""
    }
}


function Invoke-SpotifyIngest {
    param(
        [int]$TrackLimit,
        [string]$Cutoff
    )

    $ingestArgs = @()
    if ($TrackLimit -gt 0) {
        $ingestArgs += @("--max-tracks", "$TrackLimit")
    }
    if ($Cutoff) {
        $ingestArgs += @("--stop-before-added-at", $Cutoff)
    }

    python .\scripts\ingest_spotify_library.py @ingestArgs
}


function Invoke-MusicBrainzEnrichment {
    param(
        [int]$QueueLimit
    )

    $exportDir = Join-Path (Resolve-Path .).Path "exports\musicbrainz"
    $resultsDir = Join-Path $exportDir "results"
    $queueCsvPath = Join-Path $exportDir "enrichment_queue.csv"

    New-Item -ItemType Directory -Path $exportDir -Force | Out-Null
    New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null

    dbt run --select stg_musicbrainz_enrichment_queue

    $dbtQueuePath = ($queueCsvPath -replace '\\', '/')
    $dbtArgs = "{output_path: $dbtQueuePath, max_unenriched: $QueueLimit}"
    dbt run-operation export_musicbrainz_enrichment_queue --args $dbtArgs

    Write-Host "Running MusicBrainz enrichment using exported queue CSV..."
    python .\scripts\enrich_musicbrainz.py --input-csv $queueCsvPath --output-dir $resultsDir --max-unenriched $QueueLimit

    python .\scripts\load_musicbrainz_csv_to_duckdb.py --input-dir $resultsDir
}


function Invoke-DbtBuildAndTest {
    dbt run
    dbt test
}


$env:DBT_PROFILES_DIR = (Resolve-Path .\profiles).Path

$maxAddedAt = Get-MaxAddedAt
Invoke-SpotifyIngest -TrackLimit $MaxTracks -Cutoff $maxAddedAt
Invoke-MusicBrainzEnrichment -QueueLimit $MaxTracks
Invoke-DbtBuildAndTest

Write-Host "Pipeline complete. Query analytics.mart_playlist_ready_tracks for playlist candidates."
