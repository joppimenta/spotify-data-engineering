{{ config(
    materialized='table'
) }}

SELECT
  track.id AS track_id,
  artist.id AS artist_id,
  artist.name AS artist_name,
  artist.href AS artist_href,
  artist.type AS artist_type,
  artist.external_urls.spotify AS artist_spotify
FROM {{ source('landing', 'landing_spotify_tracks') }},
UNNEST(track.artists) AS artist