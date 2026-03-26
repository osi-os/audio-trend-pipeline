/* @bruin
name: audio_trends.stg_podcast_shows
type: bq.sql
materialization:
  type: table

depends:
  - audio_trends.load_raw_data

columns:
  - name: podcast_id
    checks:
      - name: not_null
  - name: category
    checks:
      - name: not_null

@bruin */

-- ---------------------------------------------------------
-- STAGING: Podcast Shows (Dimension Table)
-- Cleans podcast metadata. Note: after the category merge in
-- our ingestion script, there can be multiple rows per podcast
-- (one per category). This is intentional — it lets us analyze
-- podcasts across all their categories.

-- After the first run:
-- The merge in ingestion created itunes_id_x and itunes_id_y
-- (duplicate columns from the podcasts + categories join).
-- We take itunes_id_x and drop itunes_id_y.
-- ratings_count came through as STRING so we cast it.
-- ---------------------------------------------------------

SELECT
    podcast_id,
    itunes_id_x                             AS itunes_id,
    title                                   AS show_title,
    author                                  AS show_author,
    description                             AS show_description,
    category,
    average_rating,
    SAFE_CAST(ratings_count AS INT64)       AS ratings_count
 
FROM `audio-patterns.audio_trends.raw_podcast_shows`
 
WHERE podcast_id IS NOT NULL
  AND category IS NOT NULL