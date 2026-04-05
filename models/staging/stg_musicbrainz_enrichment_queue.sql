{{ config(
    static_analysis = 'off',
    tags = ['enrichment_feed'],
    description="""Pipeline role: Enrichment feeder staging model that queues tracks needing MusicBrainz data and is exported to CSV for the Python enrichment process.
Medallion layer: Bronze (staging bridge between raw ingestion data and enrichment execution)."""
) }}

WITH failure_state AS (
    SELECT
        UPPER(TRIM(isrc)) AS isrc,
        COALESCE(MAX(error_404_count), 0) AS error_404_count,
        MAX(TRY_CAST(last_failed_at AS TIMESTAMP)) AS last_failed_at
    FROM
        {{ source(
            'spotify_raw',
            'musicbrainz_isrc_failures'
        ) }}
    WHERE
        COALESCE(TRIM(isrc), '') <> ''
    GROUP BY
        1
),
pending_tracks AS (
    SELECT
        s.track_id,
        UPPER(TRIM(s.isrc)) AS isrc,
        TRY_CAST(s.added_at AS TIMESTAMP) AS added_at_ts,
        COALESCE(f.error_404_count, 0) AS error_404_count
    FROM
        {{ source(
            'spotify_raw',
            'spotify_saved_tracks'
        ) }}
        s
        LEFT JOIN failure_state f
        ON UPPER(TRIM(s.isrc)) = f.isrc
    WHERE
        COALESCE(TRIM(s.isrc), '') <> ''
        AND COALESCE(f.error_404_count, 0) < 10
        AND (
            f.last_failed_at IS NULL
            OR f.last_failed_at <= CURRENT_TIMESTAMP - INTERVAL '7 days'
        )
        AND (
            NOT EXISTS (
                SELECT
                    1
                FROM
                    {{ source(
                        'spotify_raw',
                        'musicbrainz_isrc_candidates'
                    ) }} C
                WHERE
                    C.track_id = s.track_id
            )
            OR EXISTS (
                SELECT
                    1
                FROM
                    {{ source(
                        'spotify_raw',
                        'musicbrainz_isrc_candidates'
                    ) }} C
                    LEFT JOIN {{ source(
                        'spotify_raw',
                        'musicbrainz_recording_payloads'
                    ) }}
                    rp
                    ON C.recording_mbid = rp.recording_mbid
                WHERE
                    C.track_id = s.track_id
                    AND rp.recording_mbid IS NULL
            )
        )
),
primary_isrc_priority AS (
    SELECT
        isrc,
        MAX(added_at_ts) AS newest_added_at,
        MAX(error_404_count) AS error_404_count
    FROM
        pending_tracks
    GROUP BY
        1
),
primary_queue AS (
    SELECT
        p.track_id,
        p.isrc,
        DENSE_RANK() over (
            ORDER BY
                pr.error_404_count ASC,
                pr.newest_added_at DESC NULLS LAST,
                p.isrc
        ) AS isrc_queue_rank,
        ROW_NUMBER() over (
            PARTITION BY p.isrc
            ORDER BY
                p.added_at_ts DESC NULLS LAST,
                p.track_id
        ) AS isrc_track_rank
    FROM
        pending_tracks p
        INNER JOIN primary_isrc_priority pr
        ON p.isrc = pr.isrc
),
recording_latest AS (
    SELECT
        recording_mbid,
        payload_json,
        ROW_NUMBER() over (
            PARTITION BY recording_mbid
            ORDER BY
                CAST(
                    last_seen_at AS TIMESTAMP
                ) DESC
        ) AS row_num
    FROM
        {{ source(
            'spotify_raw',
            'musicbrainz_recording_payloads'
        ) }}
),
recording_work_links AS (
    SELECT
        rl.recording_mbid,
        json_extract_string(
            rel.value,
            '$.work.id'
        ) AS work_mbid
    FROM
        recording_latest rl
        LEFT JOIN unnest(json_extract(rl.payload_json, '$."relation-list"[*]')) rb(value)
        ON TRUE
        LEFT JOIN unnest(json_extract(rb.value, '$.relation[*]')) rel(value)
        ON TRUE
    WHERE
        rl.row_num = 1
        AND json_extract_string(
            rb.value,
            '$."target-type"'
        ) = 'work'
        AND COALESCE(TRIM(json_extract_string(rel.value, '$.work.id')), '') <> ''
),
retry_queue AS (
    SELECT
        C.track_id,
        UPPER(TRIM(C.isrc)) AS isrc,
        TRY_CAST(s.added_at AS TIMESTAMP) AS added_at_ts,
        COALESCE(f.error_404_count, 0) AS error_404_count
    FROM
        {{ source(
            'spotify_raw',
            'musicbrainz_isrc_candidates'
        ) }} C
        INNER JOIN {{ source(
            'spotify_raw',
            'spotify_saved_tracks'
        ) }} s
        ON C.track_id = s.track_id
        LEFT JOIN failure_state f
        ON UPPER(TRIM(C.isrc)) = f.isrc
        INNER JOIN recording_work_links rwl
        ON C.recording_mbid = rwl.recording_mbid
        LEFT JOIN {{ source(
            'spotify_raw',
            'musicbrainz_work_payloads'
        ) }}
        wp
        ON rwl.work_mbid = wp.work_mbid
    WHERE
        COALESCE(TRIM(C.isrc), '') <> ''
        AND COALESCE(f.error_404_count, 0) < 10
        AND (
            f.last_failed_at IS NULL
            OR f.last_failed_at <= CURRENT_TIMESTAMP - INTERVAL '7 days'
        )
        AND wp.work_mbid IS NULL
    GROUP BY
        1,
        2,
        3,
        4
),
retry_isrc_priority AS (
    SELECT
        isrc,
        MAX(added_at_ts) AS newest_added_at,
        MAX(error_404_count) AS error_404_count
    FROM
        retry_queue
    GROUP BY
        1
),
retry_queue_ranked AS (
    SELECT
        r.track_id,
        r.isrc,
        DENSE_RANK() over (
            ORDER BY
                rp.error_404_count ASC,
                rp.newest_added_at DESC NULLS LAST,
                r.isrc
        ) AS isrc_queue_rank,
        ROW_NUMBER() over (
            PARTITION BY r.isrc
            ORDER BY
                r.added_at_ts DESC NULLS LAST,
                r.track_id
        ) AS isrc_track_rank
    FROM
        retry_queue r
        INNER JOIN retry_isrc_priority rp
        ON r.isrc = rp.isrc
),
queue AS (
    SELECT
        track_id,
        isrc,
        isrc_queue_rank,
        isrc_track_rank
    FROM
        primary_queue
    UNION ALL
    SELECT
        track_id,
        isrc,
        isrc_queue_rank,
        isrc_track_rank
    FROM
        retry_queue_ranked
    WHERE
        NOT EXISTS (
            SELECT
                1
            FROM
                primary_queue
        )
)
SELECT
    track_id as id,
    track_id,
    isrc,
    isrc_queue_rank,
    isrc_track_rank
FROM
    queue
