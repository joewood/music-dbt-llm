{{
    config(
        materialized='table'
    )
}}

select
    *
from {{ ref('int_playlist_entity_context_base') }}

