with yt as (
    select * from {{ ref('stg_youtube') }}
),

labeled AS (
  SELECT
    video_id,
    title,
    description,
    channel_id,
    channel_title,
    published_at,
    category_id,
    default_audio_language,
    tags,
    topic_categories,
    detected_topics,
    detected_language,
    category_name,
    view_count,
    like_count,
    comment_count,
    engagement_rate,
    engagement_ratio_raw,
    duration_seconds,
    days_since_published,

    /* Simple feature engineering */
    char_length(title)                                        AS title_len,
    CASE WHEN title ~ '\d' THEN true ELSE false END           AS title_has_numbers,
    EXTRACT(HOUR FROM published_at)                           AS hour_of_day,
    EXTRACT(DOW FROM published_at)                            AS day_of_week,

    CASE
      WHEN duration_seconds IS NULL THEN 'unknown'
      WHEN duration_seconds < 60 THEN 'short'
      WHEN duration_seconds < 300 THEN 'medium'
      WHEN duration_seconds < 900 THEN 'long'
      ELSE 'very_long'
    END                                                       AS content_length_bucket,

    CASE
      WHEN engagement_ratio_raw >= 0.10 THEN 'high'
      WHEN engagement_ratio_raw >= 0.05 THEN 'medium'
      WHEN engagement_ratio_raw >= 0.01 THEN 'low'
      ELSE 'very_low'
    END                                                       AS engagement_tier,

    /* Trending heuristic: fresh and non-trivial views */
    CASE
      WHEN days_since_published <= 7 AND view_count > 1000 THEN true
      ELSE false
    END                                                       AS is_trending

  FROM yt
)

SELECT * FROM labeled