{{
    config(
        materialized='table'
    )
}}

select
    a.artist_id as id,
    a.artist_id,
    a.artist_name,
    count(distinct a.track_id) as saved_track_count,
    avg(cast(s.popularity as double)) as avg_popularity,
    min(cast(s.added_at as timestamp)) as first_saved_at,
    max(cast(s.added_at as timestamp)) as last_saved_at
from {{ ref('stg_spotify_track_artists') }} a
left join {{ ref('stg_spotify_saved_tracks') }} s
    on a.track_id = s.track_id
group by 1, 2, 3

