-- Top 10 artists in the saved library by number of saved tracks.
select
    artist_id,
    artist_name,
    saved_track_count,
    avg_popularity,
    first_saved_at,
    last_saved_at
from {{ ref('fct_artist_stats') }}
order by
    saved_track_count desc,
    artist_name asc,
    artist_id asc
limit 10
