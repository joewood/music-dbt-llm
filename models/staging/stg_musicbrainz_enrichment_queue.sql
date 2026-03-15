{{ config(
    static_analysis = 'off'
) }}

WITH pending_tracks AS (

    SELECT
        s.track_id,
        UPPER(TRIM(s.isrc)) AS isrc
    FROM
        {{ source(
            'spotify_raw',
            'spotify_saved_tracks'
        ) }}
        s
    WHERE
        COALESCE(TRIM(s.isrc), '') <> ''
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
primary_queue AS (
    SELECT
        track_id,
        isrc,
        DENSE_RANK() over (
            ORDER BY
                isrc
        ) AS isrc_queue_rank,
        ROW_NUMBER() over (
            PARTITION BY isrc
            ORDER BY
                track_id
        ) AS isrc_track_rank
    FROM
        pending_tracks
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
        LEFT JOIN json_each(json_extract(rl.payload_json, '$."relation-list"')) rb
        ON TRUE
        LEFT JOIN json_each(json_extract(rb.value, '$.relation')) rel
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
        UPPER(TRIM(C.isrc)) AS isrc
    FROM
        {{ source(
            'spotify_raw',
            'musicbrainz_isrc_candidates'
        ) }} C
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
        AND wp.work_mbid IS NULL
    GROUP BY
        1,
        2
),
retry_queue_ranked AS (
    SELECT
        track_id,
        isrc,
        DENSE_RANK() over (
            ORDER BY
                isr
        ) AS isrc_queue_rank,
        ROW_NUMBER() over (
            PARTITION BY isrc
            ORDER BY
                track_id
        ) AS isrc_track_rank
    FROM
        retry_queue
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
    track_id,
    isrc,
    isrc_queue_rank,
    isrc_track_rank
FROM
    queue
