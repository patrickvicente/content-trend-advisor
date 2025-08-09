with src as (
    select * from {{ ref('stg_youtube') }}
),
feat as (
    select
        video_id, title, channel_title, published_at,
        view_count, like_count, comment_count,
        coalesce((like_count + comment_count)::float / nullif(view_count, 0), 0.0) as engagement_ratio,
        length(title) as title_len,
        (regexp_match(title, '\d+') is not null) as has_numbers,
        extract(hour from published_at)::smallint as hour_bucket,
        (current_date - date(published_at))::int as age_days,
        now() as created_at
    from src
)
select * from feat