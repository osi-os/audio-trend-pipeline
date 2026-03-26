/* @bruin
name: audio_trends.stg_podcast_charts
type: bq.sql
materialization:
  type: table

depends:
  - audio_trends.load_raw_data

columns:
  - name: chart_date
    checks:
      - name: not_null
  - name: rank
    checks:
      - name: positive

@bruin */

-- ---------------------------------------------------------
-- STAGING: Podcast Charts
-- Cleans the daily Spotify podcast rankings.
-- Note: the raw data has dot-notation columns (show.name)
-- which BigQuery imports as nested fields or with underscores.
-- We rename everything to clean snake_case.

-- After the first run:
-- date is INTEGER (Unix epoch). duration_ms and
-- show_total_episodes are FLOAT, so we cast to INT64.
-- release_date is STRING, so we PARSE it.
-- Column names already have underscores (fixed in ingestion).
-- ---------------------------------------------------------

SELECT
    DATE(TIMESTAMP_MICROS(CAST(date / 1000 AS INT64)))       AS chart_date,
    rank,
    region,
    chartRankMove                       AS chart_rank_move,
    episodeUri                          AS episode_uri,
    showUri                             AS show_uri,
    episodeName                         AS episode_name,
    show_name,
    show_publisher,
    show_description,
    show_media_type,
    CAST(show_total_episodes AS INT64)  AS show_total_episodes,
    CAST(duration_ms AS INT64)          AS duration_ms,
    explicit                            AS is_explicit,
    languages,
    SAFE.PARSE_DATE('%Y-%m-%d', release_date) AS release_date,

    -- Time dimensions for aggregation
    FORMAT_DATE('%Y-%m', DATE(TIMESTAMP_MICROS(CAST(date / 1000 AS INT64)))) AS chart_month

FROM `audio-patterns.audio_trends.raw_podcast_charts`

WHERE date IS NOT NULL
  AND rank IS NOT NULL