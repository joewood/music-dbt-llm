{{ config(
    materialized='table',
    tags=['post_enrichment'],
    description="""Pipeline role: Intermediate enrichment bridge model that explodes and normalizes track-to-instrument relationships from recording metadata.
Medallion layer: Silver (enrichment/intermediate layer)."""
) }}

with track_instruments as (
    select
        m.track_id,
        md.instrument_list_json
    from {{ ref('stg_spotify_musicbrainz_map') }} m
    inner join {{ ref('stg_musicbrainz_recording_metadata') }} md
        on m.recording_mbid = md.recording_mbid
),
exploded as (
    select
        ti.track_id,
        json_extract_string(i.value, '$') as raw_instrument
    from track_instruments ti
    left join unnest(json_extract(ti.instrument_list_json, '$[*]')) i(value)
        on true
),
normalized as (
    select
        track_id,
        trim(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(lower(coalesce(raw_instrument, '')), '<[^>]*>', '', 'g'),
                        '\\[[^\\]]*\\]|\\([^\\)]*\\)',
                        '',
                        'g'
                    ),
                    '[^a-z0-9\\s]',
                    ' ',
                    'g'
                ),
                '\\s+',
                ' ',
                'g'
            )
        ) as instrument_name
    from exploded
)
select distinct
    track_id,
    instrument_name
from normalized
where instrument_name <> ''
order by
    track_id,
    instrument_name
