WITH src AS (
  SELECT 
    external_id AS video_id,
    fetched_at,
    payload
  FROM {{ source('raw_data', var('youtube_raw_table')) }}
  WHERE source = 'youtube'
), 

clean AS (
  SELECT
    video_id,

    /* Titles and descriptions: prefer localized when present */
    COALESCE(payload->'snippet'->'localized'->>'title', payload->'snippet'->>'title') AS title,
    LEFT(COALESCE(payload->'snippet'->'localized'->>'description', payload->'snippet'->>'description', ''), 1000) AS description,

    /* Channel and timing info */
    payload->'snippet'->>'channelId' AS channel_id,
    payload->'snippet'->>'channelTitle' AS channel_title,
    (payload->'snippet'->>'publishedAt')::timestamptz AS published_at,

    /* Category and language (note: YouTube uses camelCase keys) */
    payload->'snippet'->>'categoryId' AS category_id,
    payload->'snippet'->>'defaultAudioLanguage' AS default_audio_language,

    /* Core metrics from statistics (->> needs quoted keys) */
    COALESCE((payload->'statistics'->>'viewCount')::bigint, 0) AS view_count,
    COALESCE((payload->'statistics'->>'likeCount')::bigint, 0) AS like_count,
    COALESCE((payload->'statistics'->>'commentCount')::bigint, 0) AS comment_count,

    /* Duration as seconds from ISO8601 duration */
    CASE
      WHEN payload->'contentDetails'->>'duration' IS NOT NULL
        THEN {{ iso8601_duration_to_seconds("(payload->'contentDetails'->>'duration')") }}
      ELSE NULL
    END AS duration_seconds,

    /* Tags, topics, categories */
    {{ youtube_extract_tags('payload') }} AS tags,

    /* _filter_metadata.detected_topics is an array; use -> (JSONB) not ->> (text) */
    COALESCE(
      ARRAY(SELECT jsonb_array_elements_text(payload->'_filter_metadata'->'detected_topics')),
      ARRAY[]::text[]
    ) AS detected_topics,

    payload->'_filter_metadata'->>'detected_language' AS detected_language,
    payload->'_filter_metadata'->>'category_name' AS category_name,

    /* topicDetails.topicCategories is an array of URLs */
    COALESCE(
      ARRAY(SELECT jsonb_array_elements_text(payload->'topicDetails'->'topicCategories')),
      ARRAY[]::text[]
    ) AS topic_categories,

    /* Derived metrics */
    {{ calculate_engagement_ratio_raw(
      "COALESCE((payload->'statistics'->>'viewCount')::bigint, 0)",
      "COALESCE((payload->'statistics'->>'likeCount')::bigint, 0)",
      "COALESCE((payload->'statistics'->>'commentCount')::bigint, 0)"
    ) }} AS engagement_ratio_raw,
    /* Keep a percentage for readability, but derived from the raw ratio */
    ROUND((
      {{ calculate_engagement_ratio_raw(
        "COALESCE((payload->'statistics'->>'viewCount')::bigint, 0)",
        "COALESCE((payload->'statistics'->>'likeCount')::bigint, 0)",
        "COALESCE((payload->'statistics'->>'commentCount')::bigint, 0)"
      ) }} * 100::numeric
    ), 2) AS engagement_rate,
    DATE_TRUNC('day', fetched_at) AS fetched_date,
    EXTRACT(DAY FROM (NOW() - (payload->'snippet'->>'publishedAt')::timestamptz)) AS days_since_published

  FROM src
)

SELECT * FROM clean