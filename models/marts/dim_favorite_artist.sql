{{
    config(
		materialized='view',
		description="""Pipeline role: Final output dimension mart that projects a single favorite-artist summary from artist-level aggregate outputs.
Medallion layer: Gold (mart/output dataset layer)."""
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
order by saved_track_count desc, avg_popularity desc, artist_id asc
limit 1

