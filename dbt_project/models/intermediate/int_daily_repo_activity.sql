-- Grain: one row per (repo_id, event_date). Feeds week-5 analysis 2
-- (activity seasonality) directly -- that analysis needs daily/hourly
-- counts by event type, which is exactly what this aggregates once so
-- every downstream query does not repeat the same GROUP BY.
--
-- daily_repo_activity_id is a manufactured surrogate key (repo_id and
-- event_date concatenated) rather than a dbt_utils.generate_surrogate_key
-- call -- this project has no dbt package dependencies, and one string
-- concatenation does not justify adding one.
--
-- Explicit ::string casts are load-bearing, not decoration: Snowflake's ||
-- returns NULL, silently, for the whole row when either side is a NUMBER or
-- DATE without one -- every row in this key was NULL before this cast was
-- added, caught by the not_null test on this column.

select
    repo_id::string || '-' || event_date::string  as daily_repo_activity_id,
    repo_id,
    event_date,
    count(*)                                    as total_events,
    count(distinct actor_id)                    as distinct_actors,
    count_if(event_type = 'PushEvent')          as push_events,
    count_if(event_type = 'PullRequestEvent')   as pull_request_events,
    count_if(event_type = 'IssuesEvent')        as issue_events,
    count_if(event_type = 'WatchEvent')         as watch_events,
    count_if(event_type = 'ForkEvent')          as fork_events,
    count_if(event_type = 'ReleaseEvent')       as release_events
from {{ ref('stg_events') }}
where repo_id is not null
group by repo_id, event_date
