/* @bruin
name: audio_trends.stg_podcast_reviews
type: bq.sql
materialization:
  type: table

depends:
  - audio_trends.load_raw_data

columns:
  - name: podcast_id
    checks:
      - name: not_null
  - name: rating
    checks:
      - name: not_null
      - name: positive
  - name: created_at
    checks:
      - name: not_null

@bruin */

-- ---------------------------------------------------------
-- STAGING: Podcast Reviews (Fact Table)
-- Cleans the review data. Each row is one review left by
-- a user for a specific podcast.

-- After the first run:
-- created_at is INTEGER (Unix epoch) — convert with
-- TIMESTAMP_SECONDS. rating is already INTEGER, no cast needed.
-- ---------------------------------------------------------

SELECT
    podcast_id,
    author_id,
    rating,
    title                                       AS review_title,
    content                                     AS review_content,
    TIMESTAMP_MICROS(CAST(created_at / 1000 AS INT64))               AS created_at,
 
  -- Extract time dimensions for easier aggregation downstream
    DATE(TIMESTAMP_MICROS(CAST(created_at / 1000 AS INT64)))         AS review_date,
    FORMAT_TIMESTAMP('%Y-%m', TIMESTAMP_MICROS(CAST(created_at / 1000 AS INT64))) AS review_month
 
FROM `audio-patterns.audio_trends.raw_podcast_reviews`
 
WHERE podcast_id IS NOT NULL
  AND created_at IS NOT NULL
  AND rating IS NOT NULL
  AND rating BETWEEN 1 AND 5
 