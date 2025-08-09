CREATE TABLE IF NOT EXISTS raw_content (
    id BIGSERIAL PRIMARY KEY, 
    source TEXT NOT NULL,   -- e.g. 'youtube', 'x', 'reddit'
    external_id TEXT NOT NULL, -- video id post id etc
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB NOT NULL
);

-- Unique constraint to prevent duplicate content
ALTER TABLE raw_content ADD CONSTRAINT IF NOT EXISTS uq_raw_content_source_extid 
    UNIQUE (source, external_id);

-- Useful indexes
CREATE INDEX IF NOT EXISTS idx_raw_content_source on raw_content (source);
CREATE INDEX IF NOT EXISTS idx_raw_content_extid on raw_content (external_id);
CREATE INDEX IF NOT EXISTS gin_raw_content_payload on raw_content USING GIN (payload);


CREATE TABLE IF NOT EXISTS stg_youtube (
    video_id TEXT PRIMARY KEY,
    title TEXT,
    channel_title TEXT,
    published_at TIMESTAMPTZ,
    view_count BIGINT,
    like_count BIGINT,
    comment_count BIGINT,
    tags TEXT[],
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stg_youtube_published ON stg_youtube(published_at);


CREATE TABLE IF NOT EXISTS features_content_engagement (
  video_id TEXT PRIMARY KEY,
  title TEXT,
  title_len INT,
  has_numbers BOOLEAN,
  hour_bucket SMALLINT,                -- 0..23
  age_days INT,
  view_count BIGINT,
  like_count BIGINT,
  comment_count BIGINT,
  engagement_ratio DOUBLE PRECISION,   -- (likes+comments)/views
  topic_label TEXT,                    -- optional NLP label
  sentiment NUMERIC,                   -- -1..1 or 0..1
  is_high_engagement BOOLEAN,          -- label for classification
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Optional performance indexes
CREATE INDEX IF NOT EXISTS idx_features_created ON features_content_engagement(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_features_label ON features_content_engagement(is_high_engagement);

