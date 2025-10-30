{{ config(
    materialized='table'
) }}

SELECT DISTINCT
    artist_id,
    artist_name,
    artist_href,
    artist_type,
    artist_spotify
FROM {{ ref('track_to_artists') }}

UNION DISTINCT

SELECT DISTINCT
    artist_id,
    artist_name,
    artist_href,
    artist_type,
    CAST(NULL AS STRING) AS artist_spotify
FROM {{ ref('album_to_artists') }}
