select
    track_id,
    cast(added_at as timestamp) as added_at,
    track_name,
    upper(trim(isrc)) as isrc,
    album_id,
    album_name,
    cast(album_release_date as date) as album_release_date,
    cast(album_total_tracks as integer) as album_total_tracks,
    cast(track_number as integer) as track_number,
    cast(disc_number as integer) as disc_number,
    cast(duration_ms as integer) as duration_ms,
    cast(explicit as boolean) as explicit,
    cast(popularity as integer) as popularity,
    cast(is_local as boolean) as is_local,
    primary_artist_id,
    primary_artist_name,
    spotify_track_url,
    cast(ingest_run_at as timestamp) as ingest_run_at,
    coalesce(cast(ingest_run_at as timestamp), cast(added_at as timestamp)) as spotify_updated_at
from {{ source('spotify_raw', 'spotify_saved_tracks') }}


