{{ config(static_analysis='off') }}

with ranked as (
    select
        recording_mbid,
        payload_json,
        cast(last_seen_at as timestamp) as last_seen_at,
        row_number() over (
            partition by recording_mbid
            order by cast(last_seen_at as timestamp) desc
        ) as row_num
    from {{ source('spotify_raw', 'musicbrainz_recording_payloads') }}
), latest as (
    select
        recording_mbid,
        payload_json,
        last_seen_at
    from ranked
    where row_num = 1
), artist_credits as (
    select
        l.recording_mbid,
        string_agg(
            json_extract_string(ac.value, '$.artist.name'),
            ', ' order by try_cast(ac.key as integer)
        ) as artist_credit_text,
        max(
            case when ac.key = '0' then json_extract_string(ac.value, '$.artist.name') end
        ) as primary_artist_name
    from latest l
    left join json_each(json_extract(l.payload_json, '$."artist-credit"')) ac on true
    group by 1
), first_release as (
    select
        l.recording_mbid,
        max(case when r.key = '0' then json_extract_string(r.value, '$.title') end) as release_title,
        max(case when r.key = '0' then try_cast(json_extract_string(r.value, '$.date') as date) end) as release_date
    from latest l
    left join json_each(json_extract(l.payload_json, '$."release-list"')) r on true
    group by 1
)
select
    l.recording_mbid,
    json_extract_string(l.payload_json, '$.title') as mb_recording_title,
    try_cast(json_extract_string(l.payload_json, '$.length') as integer) as mb_length_ms,
    ac.primary_artist_name as mb_primary_artist_name,
    ac.artist_credit_text as mb_artist_credit_text,
    fr.release_title as mb_release_title,
    fr.release_date as mb_release_date,
    json_extract_string(l.payload_json, '$.disambiguation') as mb_disambiguation,
    l.last_seen_at,
    l.last_seen_at as musicbrainz_recording_updated_at
from latest l
left join artist_credits ac
    on l.recording_mbid = ac.recording_mbid
left join first_release fr
    on l.recording_mbid = fr.recording_mbid
