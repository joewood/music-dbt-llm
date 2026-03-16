-- Playlist seed: tracks where normalized extracted instruments include harp.
with harp_instruments as (
    select distinct
        track_id
    from {{ ref('int_track_instrument') }}
    where regexp_matches(lower(coalesce(instrument_name, '')), '\bharp\b')
), harp_tracks as (
    select
        p.track_id,
        p.track_name,
        p.artist_names,
        p.playlist_fit_score
    from {{ ref('fct_playlist_ready_tracks') }} p
    inner join harp_instruments hi
        on p.track_id = hi.track_id
)
select distinct
    track_id,
    track_name,
    split_part(artist_names, ', ', 1) as primary_artist_name
from harp_tracks
order by primary_artist_name, track_name, track_id
