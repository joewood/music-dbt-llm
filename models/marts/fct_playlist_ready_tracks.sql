{{
    config(
        materialized='table',
        description="""Pipeline role: Final output fact mart for playlist ranking, projecting track-level scoring features and denormalized artist/enrichment attributes.
Medallion layer: Gold (mart/output dataset layer)."""
    )
}}

select
    id,
    track_id,
    track_name,
    isrc,
    artist_names,
    album_name,
    album_release_date,
    added_at,
    spotify_updated_at,
    duration_ms,
    explicit,
    popularity,
    recording_mbid,
    musicbrainz_map_updated_at,
    mb_recording_title,
    mb_primary_artist_name,
    mb_release_title,
    mb_release_date,
    mb_disambiguation,
    musicbrainz_recording_updated_at,
    top_genre,
    top_style,
    genre_list_json,
    style_list_json,
    instrument_list_json,
    primary_work_mbid,
    primary_work_title,
    work_writer_list_json,
    is_cover,
    cover_of_recording_mbid,
    cover_of_recording_title,
    covered_by_recordings_json,
    musicbrainz_metadata_updated_at,
    updated_at,
    days_since_added,
    freshness_score,
    playlist_fit_score
from {{ ref('int_playlist_ready_tracks_base') }}

