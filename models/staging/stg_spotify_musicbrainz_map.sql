{{ config(
    tags=['post_enrichment'],
    description="""Pipeline role: Staging crosswalk that picks the best MusicBrainz recording candidate per Spotify track for downstream enrichment and marts.
Medallion layer: Bronze (staging/conformance and unpacking over enrichment matching outputs)."""
) }}

with ranked as (
    select
        track_id,
        upper(trim(isrc)) as isrc,
        recording_mbid,
        'isrc' as match_source,
        match_rank,
        cast(matched_at as timestamp) as matched_at,
        row_number() over (
            partition by track_id
            order by
                coalesce(match_rank, 999999) asc,
                cast(matched_at as timestamp) desc
        ) as row_num
    from {{ source('spotify_raw', 'musicbrainz_isrc_candidates') }}
)
select
    track_id as id,
    track_id,
    isrc,
    recording_mbid,
    match_source,
    match_rank,
    matched_at,
    matched_at as musicbrainz_map_updated_at
from ranked
where row_num = 1
