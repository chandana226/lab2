-- models/output/genre_summary.sql

SELECT
  main_genre,
  COUNT(*) AS num_songs,
  SUM(streams) AS total_streams,
  ROUND(AVG(daily_streams), 2) AS avg_daily_streams
FROM {{ ref('cleaned_spotify') }}
GROUP BY main_genre
ORDER BY total_streams DESC

