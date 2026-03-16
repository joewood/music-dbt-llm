{{
    config(
        materialized='view'
    )
}}

select
	id,
	artist_id,
	artist_name,
	saved_track_count,
	avg_popularity,
	first_saved_at,
	last_saved_at
from {{ ref('fct_artist_stats') }}
order by saved_track_count desc, avg_popularity desc
limit 1

