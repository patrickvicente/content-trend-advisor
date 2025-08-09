with base as (
  select
    (payload->>'id')::text as video_id,
    payload->'snippet'->>'title' as title,
    payload->'snippet'->>'channelTitle' as channel_title,
    (payload->'snippet'->>'publishedAt')::timestamptz as published_at,
    (payload->'statistics'->>'viewCount')::bigint as view_count,
    (payload->'statistics'->>'likeCount')::bigint as like_count,
    (payload->'statistics'->>'commentCount')::bigint as comment_count,
    coalesce(
      array(select jsonb_array_elements_text(payload->'snippet'->'tags')),
      ARRAY[]::text[]
    ) as tags,
    fetched_at
  from {{ ref('raw_content_source_youtube') }}
)
select * from base;