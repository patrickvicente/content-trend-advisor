with yt as (
    select * from {{ ref('stg_youtube') }}
),

enriched AS (
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
    is_short,  -- Enhanced detection using multiple indicators
    days_since_published,
    channel_subscriber_count,
    yt_trending_seen,

    /* Simple feature engineering */
    char_length(title)                                        AS title_len,
    CASE WHEN title ~ '\d' THEN true ELSE false END           AS title_has_numbers,
    EXTRACT(HOUR FROM published_at)                           AS hour_of_day,
    EXTRACT(DOW FROM published_at)                            AS day_of_week,

    /* Shorts-specific features */
    CASE WHEN is_short THEN 'shorts' ELSE 'regular' END AS content_type,

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

    /* Age and velocity */
    GREATEST(EXTRACT(EPOCH FROM (NOW() - published_at)) / 3600.0, 0.0)  AS age_hours,
    GREATEST(EXTRACT(EPOCH FROM (NOW() - published_at)) / 3600.0, 6.0)  AS age_hours_capped,
    CASE
      WHEN EXTRACT(EPOCH FROM (NOW() - published_at)) / 3600.0 < 24  THEN '0_24h'
      WHEN EXTRACT(EPOCH FROM (NOW() - published_at)) / 3600.0 < 72  THEN '24_72h'
      WHEN EXTRACT(EPOCH FROM (NOW() - published_at)) / 3600.0 < 168 THEN '3_7d'
      ELSE '7d_plus'
    END                                                       AS age_bucket,
    (view_count::numeric / NULLIF(GREATEST(EXTRACT(EPOCH FROM (NOW() - published_at)) / 3600.0, 6.0), 0))
                                                              AS views_per_hour,
    CASE WHEN channel_subscriber_count IS NOT NULL AND channel_subscriber_count > 0
         THEN (view_count::numeric / NULLIF(GREATEST(EXTRACT(EPOCH FROM (NOW() - published_at)) / 3600.0, 6.0), 0))
              / (channel_subscriber_count / 1000.0)
         ELSE NULL
    END                                                       AS views_per_hour_per_1k_subs

  FROM yt
),

scored AS (
  SELECT
    e.*,
    percent_rank() OVER (PARTITION BY e.category_id, e.age_bucket ORDER BY e.views_per_hour) AS vph_pct_in_category,
    percent_rank() OVER (PARTITION BY e.channel_id,  e.age_bucket ORDER BY e.views_per_hour) AS vph_pct_in_channel,

    CASE
      WHEN (
        (percent_rank() OVER (PARTITION BY e.category_id, e.age_bucket ORDER BY e.views_per_hour) >= 0.90
         OR percent_rank() OVER (PARTITION BY e.channel_id,  e.age_bucket ORDER BY e.views_per_hour) >= 0.95)
        AND e.engagement_ratio_raw >= 0.01
        AND e.views_per_hour >= CASE 
                                  WHEN e.is_short THEN  -- Shorts have different velocity patterns
                                    CASE e.age_bucket
                                      WHEN '0_24h'  THEN 1000  -- Shorts can go viral faster
                                      WHEN '24_72h' THEN 500
                                      WHEN '3_7d'   THEN 200
                                      ELSE 100
                                    END
                                  ELSE  -- Regular videos
                                    CASE e.age_bucket
                                      WHEN '0_24h'  THEN 200
                                      WHEN '24_72h' THEN 100
                                      WHEN '3_7d'   THEN 50
                                      ELSE 25
                                    END
                                END
        AND e.view_count >= CASE WHEN e.is_short THEN 1000 ELSE 3000 END  -- Lower threshold for shorts
      ) THEN true ELSE false
    END                                                       AS is_trending,

    CASE
      WHEN yt_trending_seen THEN 'yt_most_popular'
      WHEN is_short AND engagement_ratio_raw < 0.005 THEN 'shorts_low_engagement'  -- Shorts need higher engagement
      WHEN NOT is_short AND engagement_ratio_raw < 0.01 THEN 'regular_low_engagement'
      WHEN is_short AND view_count < 1000 THEN 'shorts_low_views'  -- Lower threshold for shorts
      WHEN NOT is_short AND view_count < 3000 THEN 'regular_low_views'
      ELSE 'velocity_percentile'
    END                                                       AS trending_reason
  FROM enriched e
)

SELECT * FROM scored