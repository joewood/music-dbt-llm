{% macro init_raw_spotify_tables() %}
  {% if target.type == 'duckdb' %}
    {% do run_query("load json") %}
    {% do run_query("create schema if not exists raw") %}

    {% do run_query("create table if not exists raw.spotify_saved_tracks (track_id varchar, added_at varchar, track_name varchar, isrc varchar, album_id varchar, album_name varchar, album_release_date varchar, album_total_tracks integer, track_number integer, disc_number integer, duration_ms integer, explicit boolean, popularity integer, is_local boolean, primary_artist_id varchar, primary_artist_name varchar, spotify_track_url varchar, ingest_run_at varchar)") %}
    {% do run_query("alter table raw.spotify_saved_tracks add column if not exists isrc varchar") %}

    {% do run_query("create table if not exists raw.spotify_track_artists (track_id varchar, artist_id varchar, artist_name varchar, artist_order integer, ingest_run_at varchar)") %}

    {% do run_query("create table if not exists raw.musicbrainz_recordings (recording_mbid varchar, title varchar, length_ms integer, primary_artist_name varchar, artist_credit_text varchar, release_title varchar, release_date varchar, disambiguation varchar, last_seen_at varchar)") %}

    {% do run_query("create table if not exists raw.spotify_musicbrainz_map (track_id varchar, isrc varchar, recording_mbid varchar, match_source varchar, match_rank integer, matched_at varchar)") %}
    {% do run_query("create table if not exists raw.musicbrainz_isrc_candidates (track_id varchar, isrc varchar, recording_mbid varchar, match_rank integer, matched_at varchar, isrc_response_json varchar)") %}
    {% do run_query("create table if not exists raw.musicbrainz_recording_payloads (recording_mbid varchar, payload_json varchar, last_seen_at varchar)") %}
    {% do run_query("create table if not exists raw.musicbrainz_work_payloads (work_mbid varchar, payload_json varchar, last_seen_at varchar)") %}
    {% do run_query("create table if not exists raw.musicbrainz_recording_metadata (recording_mbid varchar, top_genre varchar, top_style varchar, genre_list_json varchar, style_list_json varchar, instrument_list_json varchar, primary_work_mbid varchar, primary_work_title varchar, work_writer_list_json varchar, is_cover boolean, cover_of_recording_mbid varchar, cover_of_recording_title varchar, covered_by_recordings_json varchar, metadata_source varchar, last_seen_at varchar)") %}
    {% do run_query("create table if not exists raw.musicbrainz_artists (artist_mbid varchar, artist_name varchar, sort_name varchar, disambiguation varchar, artist_type varchar, artist_country varchar, artist_gender varchar, credit_order integer, source_recording_mbid varchar, last_seen_at varchar)") %}
    {% do run_query("create table if not exists raw.musicbrainz_releases (release_mbid varchar, release_title varchar, release_date varchar, release_country varchar, release_status varchar, release_group_mbid varchar, release_group_title varchar, release_group_primary_type varchar, release_group_secondary_types_json varchar, source_recording_mbid varchar, last_seen_at varchar)") %}
    {% do run_query("create table if not exists raw.musicbrainz_works (work_mbid varchar, work_title varchar, work_type varchar, work_language varchar, disambiguation varchar, last_seen_at varchar)") %}
    {% do run_query("create table if not exists raw.musicbrainz_work_writers (work_mbid varchar, artist_mbid varchar, artist_name varchar, writer_role varchar, attributes_json varchar, last_seen_at varchar)") %}
    {% do run_query("alter table raw.musicbrainz_recording_metadata add column if not exists primary_work_mbid varchar") %}
    {% do run_query("alter table raw.musicbrainz_recording_metadata add column if not exists primary_work_title varchar") %}
    {% do run_query("alter table raw.musicbrainz_recording_metadata add column if not exists work_writer_list_json varchar") %}
    {% do run_query("alter table raw.musicbrainz_recording_metadata add column if not exists is_cover boolean") %}
    {% do run_query("alter table raw.musicbrainz_recording_metadata add column if not exists cover_of_recording_mbid varchar") %}
    {% do run_query("alter table raw.musicbrainz_recording_metadata add column if not exists cover_of_recording_title varchar") %}
    {% do run_query("alter table raw.musicbrainz_recording_metadata add column if not exists covered_by_recordings_json varchar") %}
  {% endif %}
{% endmacro %}
