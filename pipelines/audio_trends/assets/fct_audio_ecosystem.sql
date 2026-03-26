/* @bruin
name: audio_trends.fct_audio_ecosystem
type: bq.sql
materialization:
  type: table

depends:
  - audio_trends.fct_music_trends
  - audio_trends.fct_podcast_trends

@bruin */

-- ---------------------------------------------------------
-- MART: Audio Ecosystem Comparison
-- The centerpiece of the project. This model aligns music
-- and podcast trends on the same monthly time axis so we
-- can explore questions like:
--
--   - When music charts get "speechier," do podcast reviews grow?
--   - Do high-energy music months correlate with certain podcast
--     categories (e.g. Sports, Comedy)?
--   - How does the overall volume of podcast activity compare
--     to music chart activity over time?
--
-- This is the table your dashboard's cross-content tiles
-- will read from.
-- ---------------------------------------------------------

WITH music_monthly AS (
    -- Aggregate music trends across all countries per month
    -- (fct_music_trends is per-country; we roll up to global)
    SELECT
        trend_month,
        SUM(total_chart_entries)     AS music_chart_entries,
        COUNT(DISTINCT country)      AS countries_active,
        SUM(unique_tracks)           AS music_unique_tracks,
        SUM(unique_artists)          AS music_unique_artists,
        ROUND(AVG(avg_popularity), 1)       AS music_avg_popularity,
        ROUND(AVG(avg_danceability), 3)     AS music_avg_danceability,
        ROUND(AVG(avg_energy), 3)           AS music_avg_energy,
        ROUND(AVG(avg_speechiness), 3)      AS music_avg_speechiness,
        ROUND(AVG(avg_valence), 3)          AS music_avg_valence,
        ROUND(AVG(avg_acousticness), 3)     AS music_avg_acousticness,
        ROUND(AVG(explicit_ratio), 3)       AS music_explicit_ratio

    FROM `audio-patterns.audio_trends.fct_music_trends`
    GROUP BY trend_month
),

podcast_monthly AS (
    -- Aggregate podcast trends across all categories per month
    SELECT
        trend_month,
        SUM(review_count)            AS podcast_total_reviews,
        SUM(podcasts_reviewed)       AS podcast_shows_reviewed,
        SUM(unique_reviewers)        AS podcast_unique_reviewers,
        ROUND(AVG(avg_rating), 2)    AS podcast_avg_rating,
        COUNT(DISTINCT category)     AS podcast_categories_active,
        MAX(total_chart_entries)     AS podcast_chart_entries,
        MAX(unique_shows_charting)   AS podcast_shows_charting,

        -- Top category by review volume each month
        ARRAY_AGG(category ORDER BY review_count DESC LIMIT 1)[OFFSET(0)]
                                     AS podcast_top_category

    FROM `audio-patterns.audio_trends.fct_podcast_trends`
    GROUP BY trend_month
)

SELECT
    COALESCE(m.trend_month, p.trend_month) AS trend_month,

    -- Music metrics
    m.music_chart_entries,
    m.countries_active,
    m.music_unique_tracks,
    m.music_unique_artists,
    m.music_avg_popularity,
    m.music_avg_danceability,
    m.music_avg_energy,
    m.music_avg_speechiness,
    m.music_avg_valence,
    m.music_avg_acousticness,
    m.music_explicit_ratio,

    -- Podcast metrics
    p.podcast_total_reviews,
    p.podcast_shows_reviewed,
    p.podcast_unique_reviewers,
    p.podcast_avg_rating,
    p.podcast_categories_active,
    p.podcast_chart_entries,
    p.podcast_shows_charting,
    p.podcast_top_category,

    -- Computed cross-content metrics
    -- Review-to-chart ratio: how much podcast engagement per music chart entry
    ROUND(
        SAFE_DIVIDE(p.podcast_total_reviews, m.music_chart_entries), 4
    ) AS podcast_to_music_ratio

FROM music_monthly m
FULL OUTER JOIN podcast_monthly p
    ON m.trend_month = p.trend_month

ORDER BY trend_month