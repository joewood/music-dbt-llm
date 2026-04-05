-- Operational MusicBrainz request metrics.
-- Definitions:
-- - pending_queue_tracks: tracks currently present in stg_musicbrainz_enrichment_queue.
-- - pending_queue_with_errors_tracks: pending tracks whose ISRC has at least one recorded failure.
-- - succeeded_tracks: tracks with ISRC that have a resolved mapping and loaded recording payload.
with base_tracks as (
    select distinct
        track_id,
        upper(trim(isrc)) as isrc
    from {{ ref('stg_spotify_saved_tracks') }}
    where coalesce(trim(isrc), '') <> ''
), queue_tracks as (
    select distinct
        track_id,
        upper(trim(isrc)) as isrc
    from {{ ref('stg_musicbrainz_enrichment_queue') }}
), failure_isrcs as (
    select distinct
        upper(trim(isrc)) as isrc
    from {{ source('spotify_raw', 'musicbrainz_isrc_failures') }}
    where coalesce(trim(isrc), '') <> ''
), succeeded_tracks as (
    select distinct
        m.track_id
    from {{ ref('stg_spotify_musicbrainz_map') }} m
    inner join {{ ref('stg_musicbrainz_recordings') }} r
        on m.recording_mbid = r.recording_mbid
), metrics as (
    select
        (select count(*) from base_tracks) as total_tracks_with_isrc,
        (select count(*) from queue_tracks) as pending_queue_tracks,
        (
            select count(*)
            from queue_tracks q
            inner join failure_isrcs f
                on q.isrc = f.isrc
        ) as pending_queue_with_errors_tracks,
        (select count(*) from succeeded_tracks) as succeeded_tracks
)
select
    total_tracks_with_isrc,
    pending_queue_tracks,
    pending_queue_with_errors_tracks,
    pending_queue_tracks - pending_queue_with_errors_tracks as pending_queue_without_errors_tracks,
    succeeded_tracks,
    round(100.0 * succeeded_tracks / nullif(total_tracks_with_isrc, 0), 2) as succeeded_pct_of_tracks_with_isrc,
    round(100.0 * pending_queue_tracks / nullif(total_tracks_with_isrc, 0), 2) as pending_pct_of_tracks_with_isrc
from metrics
