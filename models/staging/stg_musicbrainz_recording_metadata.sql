{{ config(static_analysis='off') }}

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
), tags as (
    select
        rl.recording_mbid,
        json_extract_string(t.value, '$.name') as tag_name,
        coalesce(try_cast(json_extract_string(t.value, '$.count') as integer), 0) as tag_count
    from recording_latest rl
    left join unnest(json_extract(rl.payload_json, '$."tag-list"[*]')) t(value) on true
    where rl.row_num = 1
      and coalesce(trim(json_extract_string(t.value, '$.name')), '') <> ''
), ranked_tags as (
    select
        recording_mbid,
        tag_name,
        tag_count,
        row_number() over (
            partition by recording_mbid
            order by tag_count desc, lower(tag_name)
        ) as tag_rank
    from tags
), tag_rollup as (
    select
        recording_mbid,
        max(case when tag_rank = 1 then tag_name end) as top_genre,
        max(case when tag_rank = 2 then tag_name end) as top_style,
        to_json(list(tag_name order by tag_count desc, lower(tag_name))) as genre_list_json,
        to_json(list(tag_name order by tag_count desc, lower(tag_name))) as style_list_json
    from ranked_tags
    group by 1
), work_links as (
    select
        rl.recording_mbid,
        row_number() over (
            partition by rl.recording_mbid
            order by lower(json_extract_string(rel.value, '$.work.id'))
        ) - 1 as rel_order,
        json_extract_string(rel.value, '$.work.id') as work_mbid,
        json_extract_string(rel.value, '$.work.title') as work_title
    from recording_latest rl
    left join unnest(json_extract(rl.payload_json, '$."relation-list"[*]')) rb(value) on true
    left join unnest(json_extract(rb.value, '$.relation[*]')) rel(value) on true
    where rl.row_num = 1
      and json_extract_string(rb.value, '$."target-type"') = 'work'
      and coalesce(trim(json_extract_string(rel.value, '$.work.id')), '') <> ''
), primary_work as (
    select
        recording_mbid,
        max(case when rel_order = 0 then work_mbid end) as primary_work_mbid,
        max(case when rel_order = 0 then work_title end) as primary_work_title
    from work_links
    group by 1
), cover_relations as (
    select
        rl.recording_mbid,
        lower(coalesce(json_extract_string(rel.value, '$.type'), '')) as rel_type,
        json_extract_string(rel.value, '$.recording.id') as related_recording_mbid,
        json_extract_string(rel.value, '$.recording.title') as related_recording_title
    from recording_latest rl
    left join unnest(json_extract(rl.payload_json, '$."relation-list"[*]')) rb(value) on true
    left join unnest(json_extract(rb.value, '$.relation[*]')) rel(value) on true
    where rl.row_num = 1
      and json_extract_string(rb.value, '$."target-type"') = 'recording'
), cover_rollup as (
    select
        recording_mbid,
        max(case when rel_type = 'cover of' then true else false end) as is_cover,
        max(case when rel_type = 'cover of' then related_recording_mbid end) as cover_of_recording_mbid,
        max(case when rel_type = 'cover of' then related_recording_title end) as cover_of_recording_title,
        to_json(
            list(
                json_object('recording_mbid', related_recording_mbid, 'title', related_recording_title)
            ) filter (where rel_type = 'covers' and coalesce(trim(related_recording_mbid), '') <> '')
        ) as covered_by_recordings_json
    from cover_relations
    group by 1
), artist_relations as (
    -- Support both MusicBrainz payload shapes:
    -- 1) artist-relation-list[*] (current)
    -- 2) relation-list[*].relation[*] with target-type='artist' (legacy)
    select
        rl.recording_mbid,
        rel.value as relation_value
    from recording_latest rl
    left join unnest(json_extract(rl.payload_json, '$."artist-relation-list"[*]')) rel(value) on true
    where rl.row_num = 1

    union all

    select
        rl.recording_mbid,
        rel.value as relation_value
    from recording_latest rl
    left join unnest(json_extract(rl.payload_json, '$."relation-list"[*]')) rb(value) on true
    left join unnest(json_extract(rb.value, '$.relation[*]')) rel(value) on true
    where rl.row_num = 1
      and json_extract_string(rb.value, '$."target-type"') = 'artist'
), instruments as (
    select
        ar.recording_mbid,
        to_json(list(distinct instrument_name order by lower(instrument_name))) as instrument_list_json
    from artist_relations ar
    left join unnest(
        coalesce(
            json_extract(ar.relation_value, '$."attribute-list"[*]'),
            json_extract(ar.relation_value, '$.attributes[*]')
        )
    ) attr(attr_value) on true
    cross join lateral (
        select
            coalesce(
                json_extract_string(attr.attr_value, '$'),
                json_extract_string(attr.attr_value, '$.attribute')
            ) as instrument_name
    ) parsed_attr
    where coalesce(trim(instrument_name), '') <> ''
      and (
          lower(coalesce(json_extract_string(ar.relation_value, '$.type'), '')) = 'instrument'
          or json_extract(ar.relation_value, '$."attribute-list"') is not null
          or json_extract(ar.relation_value, '$.attributes') is not null
      )
    group by 1
)
select
    rl.recording_mbid as id,
    rl.recording_mbid,
    tr.top_genre,
    tr.top_style,
    tr.genre_list_json,
    tr.style_list_json,
    coalesce(i.instrument_list_json, to_json(json('[]'))) as instrument_list_json,
    pw.primary_work_mbid,
    pw.primary_work_title,
    coalesce(cr.is_cover, false) as is_cover,
    cr.cover_of_recording_mbid,
    cr.cover_of_recording_title,
    coalesce(cr.covered_by_recordings_json, to_json(json('[]'))) as covered_by_recordings_json,
    'musicbrainz_api' as metadata_source,
    rl.last_seen_at,
    rl.last_seen_at as musicbrainz_metadata_updated_at
from recording_latest rl
left join tag_rollup tr
    on rl.recording_mbid = tr.recording_mbid
left join primary_work pw
    on rl.recording_mbid = pw.recording_mbid
left join cover_rollup cr
    on rl.recording_mbid = cr.recording_mbid
left join instruments i
    on rl.recording_mbid = i.recording_mbid
where rl.row_num = 1
