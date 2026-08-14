-- Grain: one row per actor_id. Feeds week-5 analysis 1 (contributor
-- retention cohorts) directly -- "of actors first seen on day N, what
-- fraction are active on day N+k" needs exactly first_seen_date per actor,
-- computed once here rather than recomputed in every retention query.

select
    actor_id,
    min(created_at)         as first_seen_at,
    max(created_at)         as last_seen_at,
    min(event_date)         as first_seen_date,
    max(event_date)         as last_seen_date,
    count(*)                as total_events
from {{ ref('stg_events') }}
where actor_id is not null
group by actor_id
