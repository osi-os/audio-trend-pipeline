/* @bruin
name: audio_trends.fct_podcast_trends
type: bq.sql
materialization:
  type: table

depends:
  - audio_trends.stg_podcast_reviews
  - audio_trends.stg_podcast_shows
  - audio_trends.stg_podcast_charts

@bruin */

-- ---------------------------------------------------------
-- MART: Podcast Trends
-- Combines podcast reviews + shows + charts into monthly
-- summaries by category. This powers the "podcast side"
-- of the dashboard.
--
-- We join reviews with shows to get the category, then
-- aggregate by month and category to see:
--   - which categories are growing in review volume
--   - how ratings shift over time per category
--   - chart presence by category
-- ---------------------------------------------------------

WITH review_trends AS (
    -- Monthly review activity by category
    SELECT
        r.review_month                  AS trend_month,
        s.category,
        COUNT(*)                        AS review_count,
        COUNT(DISTINCT r.podcast_id)    AS podcasts_reviewed,
        COUNT(DISTINCT r.author_id)     AS unique_reviewers,
        ROUND(AVG(r.rating), 2)         AS avg_rating,
        -- Rating distribution (useful for dashboard)
        COUNTIF(r.rating = 5)           AS five_star_count,
        COUNTIF(r.rating = 1)           AS one_star_count

    FROM `audio-patterns.audio_trends.stg_podcast_reviews` r
    JOIN `audio-patterns.audio_trends.stg_podcast_shows` s
        ON r.podcast_id = s.podcast_id

    GROUP BY r.review_month, s.category
),

chart_trends AS (
    -- Monthly chart presence by show (we'll aggregate to approximate categories)
    SELECT
        chart_month                     AS trend_month,
        COUNT(*)                        AS chart_entries,
        COUNT(DISTINCT show_name)       AS unique_shows_charting,
        ROUND(AVG(duration_ms) / 60000.0, 1) AS avg_episode_duration_minutes

    FROM `audio-patterns.audio_trends.stg_podcast_charts`
    GROUP BY chart_month
)

-- Combine review trends with chart trends
-- Reviews give us category-level detail; charts give us overall podcast momentum
SELECT
    rt.trend_month,
    rt.category,
    rt.review_count,
    rt.podcasts_reviewed,
    rt.unique_reviewers,
    rt.avg_rating,
    rt.five_star_count,
    rt.one_star_count,

    -- Chart context (joined at month level since charts don't have category)
    ct.chart_entries            AS total_chart_entries,
    ct.unique_shows_charting,
    ct.avg_episode_duration_minutes

FROM review_trends rt
LEFT JOIN chart_trends ct
    ON rt.trend_month = ct.trend_month

ORDER BY rt.trend_month, rt.review_count DESC