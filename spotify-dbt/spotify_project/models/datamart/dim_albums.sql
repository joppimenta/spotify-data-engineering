{{ config(
    materialized='table'
) }}

SELECT DISTINCT
    album_id,
    album_name,
    album_type,
    album_release_date,
    album_release_date_precision,
    album_total_tracks
FROM {{ ref('listened_tracks') }}
