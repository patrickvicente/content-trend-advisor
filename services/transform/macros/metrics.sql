{% macro calculate_engagement_rate(views, likes, comments) %}
  CASE
    WHEN ({{ views }})::numeric > 0
      THEN ROUND(((COALESCE({{ likes }}, 0))::numeric + (COALESCE({{ comments }}, 0))::numeric) / ({{ views }})::numeric * 100::numeric, 2)
    ELSE 0::numeric
  END
{% endmacro %}

{% macro calculate_engagement_ratio_raw(views, likes, comments) %}
  CASE
    WHEN ({{ views }})::numeric > 0
      THEN ((COALESCE({{ likes }}, 0))::numeric + (COALESCE({{ comments }}, 0))::numeric) / ({{ views }})::numeric
    ELSE 0::numeric
  END
{% endmacro %}

{# Parse ISO8601 duration like 'PT18M2S' into seconds #}
{% macro iso8601_duration_to_seconds(duration_text) %}
  (
    COALESCE( (regexp_match({{ duration_text }}, 'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'))[1], '0')::int * 3600
  + COALESCE( (regexp_match({{ duration_text }}, 'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'))[2], '0')::int * 60
  + COALESCE( (regexp_match({{ duration_text }}, 'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'))[3], '0')::int
  )
{% endmacro %}

{# Extract snippet.tags as text[]; returns empty array when missing #}
{% macro youtube_extract_tags(payload_col) %}
  COALESCE(
    ARRAY(
      SELECT jsonb_array_elements_text({{ payload_col }}->'snippet'->'tags')
    ),
    ARRAY[]::text[]
  )
{% endmacro %}