{{ config(
    materialized='table'  
) }}

SELECT 

track.album.id as album_id,
artist.id as artist_id,
artist.name as artist_name,
artist.href as artist_href,
artist.type as artist_type,
artist.uri as artist_uri

FROM {{ source('landing', 'landing_spotify_tracks') }},
UNNEST(track.album.artists) AS artist