-- models/input/cleaned_spotify.sql
SELECT 
  id,
  artist_title,
  artist,
  streams,
  daily_streams,
  year,
  main_genre,
  genres,
  first_genre,
  second_genre,
  third_genre
FROM {{ source('raw', 'top_streamed_songs') }}
WHERE year >= 2010

