-- Duplicate definition:
-- 1) Same normalized primary artist name
-- 2) Same normalized track title
-- 3) Duration within 3 seconds of neighboring rows in that artist/title set
--
-- Single metric exposed by this model:
-- duplicate_occurrences = number of tracks in the library that match the
-- duplicate definition above.
with params as (
    select 3000 as duration_tolerance_ms
), base as (
    select
        track_id,
        track_name,
        primary_artist_name,
        album_name,
        album_release_date,
        added_at,
        duration_ms,
        lower(trim(coalesce(primary_artist_name, ''))) as artist_norm,
        lower(trim(coalesce(track_name, ''))) as title_norm
    from {{ ref('stg_spotify_saved_tracks') }}
    where coalesce(trim(primary_artist_name), '') <> ''
      and coalesce(trim(track_name), '') <> ''
      and duration_ms is not null
), ordered as (
    select
        b.*,
        lag(duration_ms) over (
            partition by artist_norm, title_norm
            order by duration_ms, track_id
        ) as prev_duration_ms
    from base b
), clustered as (
    select
        o.*,
        sum(
            case
                when prev_duration_ms is null then 1
                when abs(duration_ms - prev_duration_ms) > (select duration_tolerance_ms from params) then 1
                else 0
            end
        ) over (
            partition by artist_norm, title_norm
            order by duration_ms, track_id
            rows between unbounded preceding and current row
        ) as duration_cluster_id
    from ordered o
), duplicate_keys as (
    select
        artist_norm,
        title_norm,
        duration_cluster_id,
        count(*) as duplicate_occurrences
    from clustered
    group by
        artist_norm,
        title_norm,
        duration_cluster_id
    having count(*) > 1
), duplicate_rows as (
    select
        c.track_id,
        c.track_name,
        c.primary_artist_name,
        c.album_name,
        c.album_release_date,
        c.added_at,
        c.duration_ms,
        d.duplicate_occurrences
    from clustered c
    inner join duplicate_keys d
        on c.artist_norm = d.artist_norm
       and c.title_norm = d.title_norm
       and c.duration_cluster_id = d.duration_cluster_id
)
select
    dr.primary_artist_name,
    dr.track_name,
    dr.duration_ms,
    dr.album_name,
    dr.album_release_date,
    dr.added_at,
    dr.track_id,
    dr.duplicate_occurrences
from duplicate_rows dr
order by
    dr.duplicate_occurrences desc,
    dr.primary_artist_name asc,
    dr.track_name asc,
    dr.duration_ms asc,
    dr.added_at desc,
    dr.track_id asc
