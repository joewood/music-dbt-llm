-- Playlist seed: tracks where normalized extracted instruments include organ-family values.
with pipe_organ_instruments as (
    select distinct
        track_id
    from {{ ref('int_track_instrument') }}
    -- where lower(coalesce(instrument_name, '')) in (
        -- 'organ',
        -- 'pipe organ',
        -- 'hammond organ',
        -- 'reed organ',
        -- 'electronic organ'
    -- )
), instrument_rollup as (
    select
        track_id,
        list(distinct instrument_name order by lower(instrument_name)) as instrument_names
    from {{ ref('int_track_instrument') }}
    group by 1
), pipe_organ_tracks as (
    select
        p.track_id,
        p.track_name,
        split_part(p.artist_names, ', ', 1) as primary_artist_name,
        p.playlist_fit_score,
        ir.instrument_names
    from {{ ref('fct_playlist_ready_tracks') }} p
    inner join pipe_organ_instruments poi
        on p.track_id = poi.track_id
    left join instrument_rollup ir
        on p.track_id = ir.track_id
)
select distinct
    track_id,
    track_name,
    primary_artist_name,
    playlist_fit_score,
    instrument_names
from pipe_organ_tracks
where primary_artist_name = 'Hans Zimmer'
order by playlist_fit_score desc, primary_artist_name, track_name, track_id
