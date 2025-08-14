
  create view "contentdb"."public_staging"."stg_youtube__dbt_tmp"
    
    
  as (
    WITH src AS (
  SELECT 
    external_id AS video_id,
    fetched_at,
    payload
  FROM "contentdb"."public"."raw_content"
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
    CASE
      WHEN payload->'snippet'->>'defaultAudioLanguage' IS NULL THEN true
      WHEN lower(payload->'snippet'->>'defaultAudioLanguage') = 'zxx' THEN true
      WHEN lower(payload->'snippet'->>'defaultAudioLanguage') LIKE 'en%' THEN true
      ELSE false
    END AS audio_language_is_english,

    /* Core metrics from statistics (->> needs quoted keys) */
    COALESCE((payload->'statistics'->>'viewCount')::bigint, 0) AS view_count,
    COALESCE((payload->'statistics'->>'likeCount')::bigint, 0) AS like_count,
    COALESCE((payload->'statistics'->>'commentCount')::bigint, 0) AS comment_count,

    /* Duration as seconds from ISO8601 duration */
    CASE
      WHEN payload->'contentDetails'->>'duration' IS NOT NULL
        THEN 
  (
    COALESCE( (regexp_match((payload->'contentDetails'->>'duration'), 'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'))[1], '0')::int * 3600
  + COALESCE( (regexp_match((payload->'contentDetails'->>'duration'), 'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'))[2], '0')::int * 60
  + COALESCE( (regexp_match((payload->'contentDetails'->>'duration'), 'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'))[3], '0')::int
  )

      ELSE NULL
    END AS duration_seconds,

    /* Tags, topics, categories */
    
  COALESCE(
    ARRAY(
      SELECT jsonb_array_elements_text(payload->'snippet'->'tags')
    ),
    ARRAY[]::text[]
  )
 AS tags,

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
    
  CASE
    WHEN (COALESCE((payload->'statistics'->>'viewCount')::bigint, 0))::numeric > 0
      THEN ((COALESCE(COALESCE((payload->'statistics'->>'likeCount')::bigint, 0), 0))::numeric + (COALESCE(COALESCE((payload->'statistics'->>'commentCount')::bigint, 0), 0))::numeric) / (COALESCE((payload->'statistics'->>'viewCount')::bigint, 0))::numeric
    ELSE 0::numeric
  END
 AS engagement_ratio_raw,
    /* Keep a percentage for readability, but derived from the raw ratio */
    ROUND((
      
  CASE
    WHEN (COALESCE((payload->'statistics'->>'viewCount')::bigint, 0))::numeric > 0
      THEN ((COALESCE(COALESCE((payload->'statistics'->>'likeCount')::bigint, 0), 0))::numeric + (COALESCE(COALESCE((payload->'statistics'->>'commentCount')::bigint, 0), 0))::numeric) / (COALESCE((payload->'statistics'->>'viewCount')::bigint, 0))::numeric
    ELSE 0::numeric
  END
 * 100::numeric
    ), 2) AS engagement_rate,
    DATE_TRUNC('day', fetched_at) AS fetched_date,
    EXTRACT(DAY FROM (NOW() - (payload->'snippet'->>'publishedAt')::timestamptz)) AS days_since_published,

    /* Channel enrichment from ETL (if present) */
    (payload->'_channel_metadata'->>'subscriberCount')::bigint               AS channel_subscriber_count,
    (payload->'_channel_metadata'->>'hiddenSubscriberCount')::boolean         AS channel_hidden_subscribers,
    (payload->'_channel_metadata'->>'videoCount')::bigint                     AS channel_video_count,

    /* Trending-source flag (if captured during ingestion) */
    COALESCE((payload->'_source_flags'->>'youtube_trending')::boolean, false) AS yt_trending_seen

  FROM src
)

SELECT * FROM clean
  );