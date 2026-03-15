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
    Write-Host "Running progressive MusicBrainz enrichment for up to 10000 unenriched ISRC values..."
    python .\scripts\enrich_musicbrainz.py --max-unenriched 10000
}


function Invoke-DbtBuildAndTest {
    dbt run
    dbt test
}


$env:DBT_PROFILES_DIR = (Resolve-Path .\profiles).Path

$maxAddedAt = Get-MaxAddedAt
Invoke-SpotifyIngest -TrackLimit $MaxTracks -Cutoff $maxAddedAt
Invoke-MusicBrainzEnrichment
Invoke-DbtBuildAndTest

Write-Host "Pipeline complete. Query analytics.mart_playlist_ready_tracks for playlist candidates."
