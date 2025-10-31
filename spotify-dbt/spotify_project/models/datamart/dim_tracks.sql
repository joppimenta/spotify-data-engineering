{{ config(
    materialized='table'
) }}

SELECT DISTINCT
  track_id,
  track_name,
  popularity,
  duration_ms,
  disc_number,
  track_number,
  type AS track_type,
  uri AS track_uri,
  album_id 
FROM {{ ref('listened_tracks') }}
