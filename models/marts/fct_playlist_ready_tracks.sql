{{
    config(
        materialized='table'
    )
}}

select
    *
from {{ ref('int_playlist_ready_tracks_base') }}

