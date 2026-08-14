-- Grain: one row per actor_id. Built from int_actor_first_last_seen (the
-- aggregation) joined back to stg_actors (the current login/display_login) --
-- consuming the intermediate layer rather than re-aggregating stg_events
-- directly is the point of having that layer at all.
--
-- Incremental + merge on actor_id: an actor active again on a later day
-- needs last_seen_at to move forward, which is an update to an existing row,
-- not a new one. Append-only would leave every actor with a stale
-- last_seen_at after their first appearance.

{{ config(
    materialized='incremental',
    unique_key='actor_id',
    incremental_strategy='merge'
) }}

select
    a.actor_id,
    a.actor_login,
    a.actor_display_login,
    s.first_seen_at,
    s.last_seen_at,
    s.first_seen_date,
    s.last_seen_date,
    s.total_events
from {{ ref('int_actor_first_last_seen') }} s
inner join {{ ref('stg_actors') }} a on a.actor_id = s.actor_id

{% if is_incremental() %}
where s.last_seen_date > (select max(last_seen_date) from {{ this }})
{% endif %}
