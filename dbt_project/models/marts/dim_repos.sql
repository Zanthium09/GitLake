-- Grain: one row per repo_id. Aggregates int_daily_repo_activity (already at
-- repo+day grain) up to repo grain, joined to stg_repos for the current
-- name/owner. Same incremental+merge reasoning as dim_actors: a repo active
-- on a later day needs its last_seen_date and total_events to update in
-- place, not accumulate as a second row.
--
-- This mart is also what snapshots/dim_repos_snapshot.sql tracks for SCD
-- Type 2 -- see that file for why the snapshot targets stg_repos rather than
-- this table directly.

{{ config(
    materialized='incremental',
    unique_key='repo_id',
    incremental_strategy='merge'
) }}

select
    r.repo_id,
    r.repo_name,
    r.repo_owner,
    r.repo_short_name,
    min(a.event_date)      as first_seen_date,
    max(a.event_date)      as last_seen_date,
    sum(a.total_events)    as total_events
from {{ ref('int_daily_repo_activity') }} a
inner join {{ ref('stg_repos') }} r on r.repo_id = a.repo_id
group by r.repo_id, r.repo_name, r.repo_owner, r.repo_short_name

{% if is_incremental() %}
having max(a.event_date) > (select max(last_seen_date) from {{ this }})
{% endif %}
