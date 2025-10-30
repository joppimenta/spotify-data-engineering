{{ config(
    materialized='table'
) }}

SELECT
    l.played_at,
    l.track_id,
    l.track_name,
    l.album_id,
    STRING_AGG(tta.artist_id, ', ') AS artist_ids,  -- concatena os artistas
    l.duration_ms,
    l.popularity,
    l.context_type,
    l.load_datetime
FROM {{ ref('listened_tracks') }} l
JOIN {{ ref('track_to_artists') }} tta
    ON l.track_id = tta.track_id
GROUP BY
    l.played_at,
    l.track_id,
    l.track_name,
    l.album_id,
    l.duration_ms,
    l.popularity,
    l.context_type,
    l.load_datetime
