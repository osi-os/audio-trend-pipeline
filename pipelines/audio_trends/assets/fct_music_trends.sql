/* @bruin
name: audio_trends.fct_music_trends
type: bq.sql
materialization:
  type: table

depends:
  - audio_trends.stg_spotify_charts

@bruin */

-- ---------------------------------------------------------
-- MART: Music Streaming Trends
-- Aggregates Spotify chart data into monthly summaries
-- by country. This powers the "music side" of the dashboard.
--
-- Key metrics:
--   - unique tracks and artists per month/country
--   - average audio features (danceability, energy, speechiness)
--   - popularity trends over time
--
-- The speechiness metric is especially interesting for our
-- cross-content analysis — high speechiness tracks blur the
-- line between music and spoken word content.
-- ---------------------------------------------------------

SELECT
    FORMAT_DATE('%Y-%m', snapshot_date)  AS trend_month,
    country,

    -- Volume metrics
    COUNT(DISTINCT spotify_id)          AS unique_tracks,
    COUNT(DISTINCT artists)             AS unique_artists,
    COUNT(*)                            AS total_chart_entries,

    -- Popularity
    ROUND(AVG(popularity), 1)           AS avg_popularity,

    -- Audio feature averages (the "sound profile" of each market)
    ROUND(AVG(danceability), 3)         AS avg_danceability,
    ROUND(AVG(energy), 3)              AS avg_energy,
    ROUND(AVG(speechiness), 3)         AS avg_speechiness,
    ROUND(AVG(acousticness), 3)        AS avg_acousticness,
    ROUND(AVG(valence), 3)             AS avg_valence,
    ROUND(AVG(tempo), 1)               AS avg_tempo,
    ROUND(AVG(instrumentalness), 3)    AS avg_instrumentalness,

    -- Explicit content ratio
    ROUND(
        COUNTIF(is_explicit = TRUE) / COUNT(*), 3
    ) AS explicit_ratio,

    -- Average track duration in minutes
    ROUND(AVG(duration_ms) / 60000.0, 1) AS avg_duration_minutes

FROM `audio-patterns.audio_trends.stg_spotify_charts`

GROUP BY trend_month, country
ORDER BY trend_month, country