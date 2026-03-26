/* @bruin
name: audio_trends.stg_spotify_charts
type: bq.sql
materialization:
  type: table

depends:
  - audio_trends.load_raw_data

columns:
  - name: spotify_id
    checks:
      - name: not_null
  - name: snapshot_date
    checks:
      - name: not_null
  - name: daily_rank
    checks:
      - name: positive

@bruin */

-- ---------------------------------------------------------
-- STAGING: Spotify Charts
-- Cleans the raw Spotify 73 Countries data.
-- Selects only the columns we need, casts types explicitly,
-- and renames for consistency across our models.
 
-- ---------------------------------------------------------
-- After the first run:
-- The raw snapshot_date came through as INTEGER (Unix epoch).
-- We convert it to a proper DATE using TIMESTAMP_SECONDS,
-- which interprets the integer as seconds since 1970-01-01.
-- album_release_date is a STRING so we PARSE it instead.
-- ---------------------------------------------------------

SELECT
    -- Track identifiers
    spotify_id,
    name AS track_name,
    artists,
    album_name,

    -- Chart position data
    daily_rank,
    daily_movement,
    weekly_movement,
    country,
    DATE(TIMESTAMP_MICROS(CAST(snapshot_date / 1000 AS INT64))) AS snapshot_date,
    
    -- Track metadata
    popularity,
    is_explicit,
    duration_ms,
    SAFE.PARSE_DATE('%Y-%m-%d', album_release_date) AS album_release_date,

    -- Audio features (these are the interesting analytical columns)
    -- Each is a float between 0.0 and 1.0 (except tempo/loudness)
    danceability,       -- how suitable for dancing (0.0 = least, 1.0 = most)
    energy,             -- intensity and activity (0.0 = calm, 1.0 = energetic)
    speechiness,        -- presence of spoken words (> 0.66 = mostly speech, like podcasts!)
    acousticness,       -- likelihood of being acoustic (0.0 = electronic, 1.0 = acoustic)
    instrumentalness,   -- predicts if track has no vocals (> 0.5 = instrumental)
    liveness,           -- presence of live audience (> 0.8 = likely live)
    valence,            -- musical positivity (0.0 = sad/angry, 1.0 = happy/cheerful)
    tempo,              -- BPM (beats per minute)
    loudness,           -- overall loudness in dB
    `key`,              -- backticks because "key" is a reserved word in SQL
    mode,               -- major (1) or minor (0)
    time_signature

FROM `audio-patterns.audio_trends.raw_spotify_charts`

-- Drop any rows missing critical fields
WHERE spotify_id IS NOT NULL
  AND snapshot_date IS NOT NULL