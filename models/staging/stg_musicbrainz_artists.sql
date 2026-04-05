{{ config(
    static_analysis='off',
    description="""Pipeline role: Staging model that normalizes MusicBrainz artist entities observed in matched recording payloads for entity-context enrichment.
Medallion layer: Silver (staging/conformance over Bronze enriched payload tables)."""
) }}

with recording_latest as (
    select
        recording_mbid,
        payload_json,
        cast(last_seen_at as timestamp) as last_seen_at,
        row_number() over (
            partition by recording_mbid
            order by cast(last_seen_at as timestamp) desc
        ) as row_num
    from {{ source('spotify_raw', 'musicbrainz_recording_payloads') }}
), exploded as (
    select
        json_extract_string(ac.value, '$.artist.id') as artist_mbid,
        json_extract_string(ac.value, '$.artist.name') as artist_name,
        json_extract_string(ac.value, '$.artist.sort-name') as sort_name,
        json_extract_string(ac.value, '$.artist.disambiguation') as disambiguation,
        json_extract_string(ac.value, '$.artist.type') as artist_type,
        json_extract_string(ac.value, '$.artist.country') as artist_country,
        json_extract_string(ac.value, '$.artist.gender') as artist_gender,
        row_number() over (
            partition by rl.recording_mbid
            order by lower(json_extract_string(ac.value, '$.artist.name'))
        ) as credit_order,
        rl.recording_mbid as source_recording_mbid,
        rl.last_seen_at
    from recording_latest rl
    left join unnest(json_extract(rl.payload_json, '$."artist-credit"[*]')) ac(value) on true
    where rl.row_num = 1
), ranked as (
    select
        *,
        row_number() over (
            partition by artist_mbid, source_recording_mbid, credit_order
            order by last_seen_at desc
        ) as row_num
    from exploded
    where coalesce(trim(artist_mbid), '') <> ''
)
select
    concat(artist_mbid, ':', source_recording_mbid, ':', cast(credit_order as varchar)) as id,
    artist_mbid,
    artist_name,
    sort_name,
    disambiguation,
    artist_type,
    artist_country,
    artist_gender,
    credit_order,
    source_recording_mbid,
    last_seen_at
from ranked
where row_num = 1
