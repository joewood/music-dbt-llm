{{ config(
    static_analysis='off',
    description="""Pipeline role: Staging model that normalizes writer/composer/lyricist relationships for MusicBrainz works used by entity enrichment.
Medallion layer: Silver (staging/conformance over Bronze enriched payload tables)."""
) }}

with work_latest as (
    select
        work_mbid,
        payload_json,
        cast(last_seen_at as timestamp) as last_seen_at,
        row_number() over (
            partition by work_mbid
            order by cast(last_seen_at as timestamp) desc
        ) as row_num
    from {{ source('spotify_raw', 'musicbrainz_work_payloads') }}
), rel_blocks as (
    select
        wl.work_mbid,
        wl.last_seen_at,
        rb.value as rel_block_value
    from work_latest wl
        left join unnest(json_extract(wl.payload_json, '$."relation-list"[*]')) rb(value) on true
    where wl.row_num = 1
      and json_extract_string(rb.value, '$."target-type"') = 'artist'
), relations as (
    select
        rb.work_mbid,
        rb.last_seen_at,
        rel.value as relation_value
    from rel_blocks rb
    left join unnest(json_extract(rb.rel_block_value, '$.relation[*]')) rel(value) on true
), filtered as (
    select
        work_mbid,
        json_extract_string(relation_value, '$.artist.id') as artist_mbid,
        json_extract_string(relation_value, '$.artist.name') as artist_name,
        lower(json_extract_string(relation_value, '$.type')) as writer_role,
        to_json(coalesce(json_extract(relation_value, '$."attribute-list"'), json('[]'))) as attributes_json,
        last_seen_at
    from relations
    where lower(coalesce(json_extract_string(relation_value, '$.type'), ''))
        in ('writer', 'composer', 'lyricist', 'librettist')
), ranked as (
    select
        *,
        row_number() over (
            partition by work_mbid, artist_mbid, writer_role
            order by last_seen_at desc
        ) as row_num
    from filtered
    where coalesce(trim(work_mbid), '') <> ''
      and coalesce(trim(artist_mbid), '') <> ''
)
select
    concat(work_mbid, ':', artist_mbid, ':', writer_role) as id,
    work_mbid,
    artist_mbid,
    artist_name,
    writer_role,
    attributes_json,
    last_seen_at
from ranked
where row_num = 1
