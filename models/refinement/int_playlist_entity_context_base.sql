{{ config(
    tags=['post_enrichment'],
    description="""Pipeline role: Intermediate enrichment model that assembles one-row-per-track entity context, release rollups, and writer rollups for final marts.
Medallion layer: Silver (enrichment/intermediate layer)."""
) }}

with artists as (
    select
        track_id,
        string_agg(artist_name, ', ' order by artist_order) as artist_names
    from {{ ref('stg_spotify_track_artists') }}
    group by 1
),
mb_map as (
    select
        track_id,
        recording_mbid,
        musicbrainz_map_updated_at
    from {{ ref('stg_spotify_musicbrainz_map') }}
),
mb_recordings as (
    select
        recording_mbid,
        mb_recording_title,
        mb_primary_artist_name,
        mb_release_title,
        mb_release_date,
        mb_disambiguation,
        musicbrainz_recording_updated_at
    from {{ ref('stg_musicbrainz_recordings') }}
),
mb_metadata as (
    select
        recording_mbid,
        top_genre,
        top_style,
        genre_list_json,
        style_list_json,
        instrument_list_json,
        primary_work_mbid,
        primary_work_title,
        is_cover,
        cover_of_recording_mbid,
        cover_of_recording_title,
        covered_by_recordings_json,
        musicbrainz_metadata_updated_at
    from {{ ref('stg_musicbrainz_recording_metadata') }}
),
release_rollup as (
    select
        source_recording_mbid as recording_mbid,
        min(release_date) as first_release_date,
        max(release_date) as latest_release_date,
        string_agg(distinct release_title, ' | ') as release_titles,
        string_agg(distinct release_group_primary_type, ', ') as release_group_types,
        max(cast(last_seen_at as timestamp)) as releases_updated_at,
        max(case when lower(coalesce(release_group_primary_type, '')) = 'single' then 1 else 0 end) = 1 as has_single_release,
        max(case when lower(coalesce(release_group_primary_type, '')) = 'album' then 1 else 0 end) = 1 as has_album_release
    from {{ ref('stg_musicbrainz_releases') }}
    group by 1
),
writer_rollup as (
    select
        work_mbid,
        string_agg(distinct artist_name, ', ') as writer_names,
        string_agg(distinct writer_role, ', ') as writer_roles,
        max(cast(last_seen_at as timestamp)) as writers_updated_at
    from {{ ref('stg_musicbrainz_work_writers') }}
    group by 1
),
base as (
    select
        s.track_id as id,
        s.track_id,
        s.track_name,
        s.isrc,
        coalesce(a.artist_names, s.primary_artist_name) as artist_names,
        s.primary_artist_name as spotify_primary_artist_name,
        s.album_name,
        s.album_release_date,
        s.added_at,
        s.spotify_updated_at,
        s.explicit,
        coalesce(s.popularity, 0) as popularity,
        mm.recording_mbid,
        mm.musicbrainz_map_updated_at,
        mr.mb_recording_title,
        mr.mb_primary_artist_name,
        mr.mb_release_title,
        mr.mb_release_date,
        mr.mb_disambiguation,
        mr.musicbrainz_recording_updated_at,
        md.top_genre,
        md.top_style,
        md.genre_list_json,
        md.style_list_json,
        md.instrument_list_json,
        md.primary_work_mbid,
        md.primary_work_title,
        md.is_cover,
        md.cover_of_recording_mbid,
        md.cover_of_recording_title,
        md.covered_by_recordings_json,
        md.musicbrainz_metadata_updated_at,
        rr.first_release_date,
        rr.latest_release_date,
        rr.release_titles,
        rr.release_group_types,
        rr.has_single_release,
        rr.has_album_release,
        rr.releases_updated_at,
        wr.writer_names,
        wr.writer_roles,
        wr.writers_updated_at,
        greatest(
            coalesce(s.spotify_updated_at, timestamp '1900-01-01'),
            coalesce(mm.musicbrainz_map_updated_at, timestamp '1900-01-01'),
            coalesce(mr.musicbrainz_recording_updated_at, timestamp '1900-01-01'),
            coalesce(md.musicbrainz_metadata_updated_at, timestamp '1900-01-01'),
            coalesce(rr.releases_updated_at, timestamp '1900-01-01'),
            coalesce(wr.writers_updated_at, timestamp '1900-01-01')
        ) as updated_at,
        date_diff('day', cast(s.added_at as date), current_date) as days_since_added
    from {{ ref('stg_spotify_saved_tracks') }} s
    left join artists a
        on s.track_id = a.track_id
    left join mb_map mm
        on s.track_id = mm.track_id
    left join mb_recordings mr
        on mm.recording_mbid = mr.recording_mbid
    left join mb_metadata md
        on mm.recording_mbid = md.recording_mbid
    left join release_rollup rr
        on mm.recording_mbid = rr.recording_mbid
    left join writer_rollup wr
        on md.primary_work_mbid = wr.work_mbid
)
select
    *,
    greatest(0, 100 - least(days_since_added, 100)) as freshness_score,
    round(
        (popularity * 0.55)
        + (greatest(0, 100 - least(days_since_added, 100)) * 0.35)
        + case when recording_mbid is not null then 4 else 0 end
        + case when has_single_release then 1.5 else 0 end
        + case when has_album_release then 1 else 0 end
        - case when explicit then 10 else 0 end,
        2
    ) as auto_playlist_score
from base
