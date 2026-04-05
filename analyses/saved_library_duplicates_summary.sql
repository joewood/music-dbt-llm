-- One row per duplicate ISRC with a compact, human-readable list of matching tracks.
with duplicate_groups as (
    select
        isrc,
        duplicate_occurrences as duplicate_count
    from {{ ref('fct_track_duplicates') }}
), track_lines as (
    select
        s.isrc,
        concat(
            coalesce(s.track_name, '<unknown track>'),
            ' - ',
            coalesce(s.primary_artist_name, '<unknown artist>'),
            ' (',
            coalesce(s.album_name, '<unknown album>'),
            ', added ',
            cast(s.added_at as varchar),
            ')'
        ) as track_line,
        s.added_at
    from {{ ref('stg_spotify_saved_tracks') }} s
)
select
    d.isrc,
    d.duplicate_count,
    string_agg(t.track_line, ' | ' order by t.added_at desc, t.track_line) as duplicate_track_list,
    min(t.added_at) as first_added_at,
    max(t.added_at) as latest_added_at
from duplicate_groups d
inner join track_lines t
    on d.isrc = t.isrc
group by
    d.isrc,
    d.duplicate_count
order by
    d.duplicate_count desc,
    latest_added_at desc,
    d.isrc
