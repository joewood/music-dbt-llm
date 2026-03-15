-- Build a diverse, high-scoring candidate pool to feed your playlist generation logic.
with ranked as (
    select
        *,
        row_number() over (
            partition by split_part(artist_names, ', ', 1)
            order by playlist_fit_score desc, popularity desc
        ) as artist_rank
    from {{ ref('mart_playlist_ready_tracks') }}
    where is_local = false
)
select
    track_id,
    track_name,
    artist_names,
    album_name,
    popularity,
    playlist_fit_score,
    added_at
from ranked
where artist_rank <= 3
order by playlist_fit_score desc
limit 200;
