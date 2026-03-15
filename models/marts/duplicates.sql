with final as (
    select 
        isrc,
        array_agg(album_name order by added_at desc) as album_names,
        array_agg(album_release_date order by added_at desc) as album_release_dates,
        count(*) as cnt
    from {{ ref("stg_spotify_saved_tracks") }}
    group by isrc
    having count(*) > 1
    order by cnt desc
)
select * from final