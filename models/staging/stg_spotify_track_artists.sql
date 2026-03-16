select
    concat(track_id, ':', cast(artist_order as varchar)) as id,
    track_id,
    artist_id,
    artist_name,
    cast(artist_order as integer) as artist_order,
    cast(ingest_run_at as timestamp) as ingest_run_at
from {{ source('spotify_raw', 'spotify_track_artists') }}
