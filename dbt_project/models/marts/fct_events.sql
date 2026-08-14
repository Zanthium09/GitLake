-- Grain: one row per event_id -- the fact table other marts and the week-5
-- analyses join against. Incremental + merge rather than a full rebuild:
-- CLAUDE.md's non-negotiable #2 is a provably idempotent backfill, and merge
-- keyed on event_id is what makes rerunning a day safe instead of doubling
-- it.
--
-- QUALIFY below is load-bearing, not defensive: MERGE only dedupes on
-- incremental runs, matching new rows against keys already in the table. The
-- first build of any incremental model is a plain CREATE TABLE AS SELECT --
-- no merge logic runs at all -- so the one confirmed upstream duplicate
-- (event_id 34822912958, byte-identical rows, see stg_events) flowed straight
-- through on the initial load and failed the unique test here. A fact table's
-- grain should hold unconditionally, not depend on which build path ran, so
-- it is enforced in the query itself.
--
-- is_incremental() filters to event_date beyond what is already loaded, so a
-- full table scan only happens on the first build.

{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge'
) }}

select
    event_id,
    event_type,
    created_at,
    event_date,
    is_public,
    actor_id,
    repo_id,
    org_id,
    action,
    ref,
    ref_type,
    push_id,
    push_size,
    push_distinct_size,
    pr_number,
    pr_id,
    pr_state,
    pr_merged,
    pr_created_at,
    pr_merged_at,
    issue_id,
    issue_number,
    issue_state,
    comment_id,
    forkee_id,
    release_id,
    member_id
from {{ ref('stg_events') }}

{% if is_incremental() %}
where event_date > (select max(event_date) from {{ this }})
{% endif %}

qualify row_number() over (
    partition by event_id
    order by created_at
) = 1
