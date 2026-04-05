-- Show duplicate ISRC groups with track-level detail for saved-library cleanup.
with duplicate_groups as (
    select
        isrc,
        duplicate_occurrences as duplicate_count
    from {{ ref('fct_track_duplicates') }}
), track_detail as (
    select
        s.track_id,
        s.track_name,
        s.primary_artist_name,
        s.album_name,
        s.album_release_date,
        s.added_at,
        s.isrc
    from {{ ref('stg_spotify_saved_tracks') }} s
)
select
    d.isrc,
    d.duplicate_count,
    t.track_id,
    t.track_name,
    t.primary_artist_name,
    t.album_name,
    t.album_release_date,
    t.added_at,
    row_number() over (
        partition by d.isrc
        order by t.added_at desc, t.track_id
    ) as duplicate_rank_in_isrc,
    min(t.added_at) over (partition by d.isrc) as first_added_at,
    max(t.added_at) over (partition by d.isrc) as latest_added_at
from duplicate_groups d
inner join track_detail t
    on d.isrc = t.isrc
order by
    d.duplicate_count desc,
    d.isrc,
    duplicate_rank_in_isrc;
