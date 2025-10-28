{{ config(
    materialized='table'  
) }}
SELECT   
  track.id AS track_id,
  track.name as track_name,
  track.popularity as popularity,
  --track.available_markets as available_markets,
  track.duration_ms as duration_ms,
  track.disc_number as disc_number,
  track.href as href,
  track.is_local as is_local,
  track.preview_url as preview_url,
  track.track_number as track_number,
  track.type as type,
  track.uri as uri,
  track.album.id as album_id,
  track.album.name as album_name,
  PARSE_DATE(
    '%Y-%m-%d',
    CASE
      WHEN LENGTH(track.album.release_date) = 4 THEN CONCAT(track.album.release_date, '-01-01')
      ELSE track.album.release_date
    END
  ) AS album_release_date,
  track.album.release_date_precision as album_release_date_precision,
  track.album.total_tracks as album_total_tracks,
  track.album.type as album_type,
  track.album.uri as album_uri,
  played_at,
  context.type as context_type,
  load_datetime


FROM {{ source('landing', 'landing_spotify_tracks') }}