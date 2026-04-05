{{
    config(
        materialized='table',
        description="""Pipeline role: Final output fact mart for duplicate diagnostics, surfacing ISRC duplicate groups for quality review and triage.
Medallion layer: Gold (mart/output dataset layer)."""
    )
}}

with final as (
    select 
        isrc as id,
        isrc,
        array_agg(album_name order by added_at desc) as album_names,
        array_agg(album_release_date order by added_at desc) as album_release_dates,
        count(*) as cnt
    from {{ ref("stg_spotify_saved_tracks") }}
    group by isrc
    having count(*) > 1
)
select
    id,
    isrc,
    album_names,
    album_release_dates,
    cnt
from final
order by cnt desc
