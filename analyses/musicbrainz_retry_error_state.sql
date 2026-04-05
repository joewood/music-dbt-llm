-- Quick operational view of MusicBrainz retry/error state.
-- Prioritizes records in current queue, then eligible retries, with lowest 404 counts first.
with failure_state as (
    select
        upper(trim(isrc)) as isrc,
        max(coalesce(error_404_count, 0)) as error_404_count,
        max(try_cast(last_failed_at as timestamp)) as last_failed_at,
        arg_max(error_status_code, try_cast(last_failed_at as timestamp)) as latest_error_status_code,
        arg_max(error_message, try_cast(last_failed_at as timestamp)) as latest_error_message
    from {{ source('spotify_raw', 'musicbrainz_isrc_failures') }}
    where coalesce(trim(isrc), '') <> ''
    group by 1
),
queue_state as (
    select
        upper(trim(isrc)) as isrc,
        min(isrc_queue_rank) as queue_rank
    from {{ ref('stg_musicbrainz_enrichment_queue') }}
    group by 1
),
joined as (
    select
        f.isrc,
        f.error_404_count,
        f.latest_error_status_code,
        f.last_failed_at,
        f.last_failed_at + interval '7 days' as next_retry_at,
        case
            when f.error_404_count >= 10 then false
            when f.last_failed_at <= current_timestamp - interval '7 days' then true
            else false
        end as eligible_now,
        case when q.isrc is not null then true else false end as currently_in_queue,
        q.queue_rank,
        f.latest_error_message
    from failure_state f
    left join queue_state q
        on f.isrc = q.isrc
)
select
    isrc,
    error_404_count,
    latest_error_status_code,
    last_failed_at,
    next_retry_at,
    eligible_now,
    currently_in_queue,
    queue_rank,
    latest_error_message
from joined
order by
    currently_in_queue desc,
    eligible_now desc,
    error_404_count asc,
    queue_rank asc nulls last,
    last_failed_at desc nulls last,
    isrc
