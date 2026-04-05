{{ config(
    static_analysis='off',
    description="""Pipeline role: Staging model that normalizes MusicBrainz release and release-group attributes used in downstream release rollups.
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
        json_extract_string(r.value, '$.id') as release_mbid,
        json_extract_string(r.value, '$.title') as release_title,
        try_cast(json_extract_string(r.value, '$.date') as date) as release_date,
        json_extract_string(r.value, '$.country') as release_country,
        json_extract_string(r.value, '$.status') as release_status,
        json_extract_string(r.value, '$."release-group".id') as release_group_mbid,
        json_extract_string(r.value, '$."release-group".title') as release_group_title,
        coalesce(
            json_extract_string(r.value, '$."release-group"."primary-type"'),
            json_extract_string(r.value, '$."release-group".type')
        ) as release_group_primary_type,
        to_json(
            coalesce(
                json_extract(r.value, '$."release-group"."secondary-type-list"'),
                json('[]')
            )
        ) as release_group_secondary_types_json,
        rl.recording_mbid as source_recording_mbid,
        rl.last_seen_at
    from recording_latest rl
    left join unnest(json_extract(rl.payload_json, '$."release-list"[*]')) r(value) on true
    where rl.row_num = 1
), ranked as (
    select
        *,
        row_number() over (
            partition by release_mbid, source_recording_mbid
            order by last_seen_at desc
        ) as row_num
    from exploded
    where coalesce(trim(release_mbid), '') <> ''
)
select
    concat(release_mbid, ':', source_recording_mbid) as id,
    release_mbid,
    release_title,
    release_date,
    release_country,
    release_status,
    release_group_mbid,
    release_group_title,
    release_group_primary_type,
    release_group_secondary_types_json,
    source_recording_mbid,
    last_seen_at
from ranked
where row_num = 1
