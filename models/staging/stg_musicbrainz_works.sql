{{ config(static_analysis='off') }}

with ranked as (
    select
        work_mbid,
        payload_json,
        cast(last_seen_at as timestamp) as last_seen_at,
        row_number() over (
            partition by work_mbid
            order by cast(last_seen_at as timestamp) desc
        ) as row_num
    from {{ source('spotify_raw', 'musicbrainz_work_payloads') }}
)
select
    work_mbid,
    json_extract_string(payload_json, '$.title') as work_title,
    json_extract_string(payload_json, '$.type') as work_type,
    json_extract_string(payload_json, '$.language') as work_language,
    json_extract_string(payload_json, '$.disambiguation') as disambiguation,
    last_seen_at
from ranked
where row_num = 1
