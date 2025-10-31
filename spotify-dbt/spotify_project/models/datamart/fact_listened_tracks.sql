{{ config(
    materialized='table'
) }}

SELECT
    l.played_at,
    l.track_id,
    l.track_name,
    l.album_id,
    l.duration_ms,
    l.popularity,
    l.context_type,
    l.load_datetime
FROM {{ ref('listened_tracks') }} l

