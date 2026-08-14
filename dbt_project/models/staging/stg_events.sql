-- Event-level grain, light cleaning only: trim, empty-string-to-null, no
-- joins and no aggregation -- that belongs in intermediate/marts.
--
-- event_id is NOT guaranteed unique in the source. GitHub Archive itself
-- emitted event 34822912958 (an IssuesEvent) as two byte-identical rows on
-- 2024-01-15 -- confirmed by hand, not a defect in this pipeline. The
-- uniqueness test on this model is expected to report that one failure;
-- see _staging.yml for why it is warn, not error.

select
    event_id,
    trim(event_type)                as event_type,
    created_at,
    -- Recomputed, not passed through: the raw table's event_date column is
    -- NULL for every row. Spark wrote it as a Hive-style partition
    -- (event_date=2024-01-15/), which means the value lives only in the S3
    -- folder name, not inside the Parquet file itself. Plain COPY INTO has
    -- no partition-path awareness -- it reads whatever columns physically
    -- exist in the file, so the raw column loaded as NULL across all
    -- 5,528,301 rows. created_at came through fine, so deriving event_date
    -- from it here is both the fix and the more honest source of truth.
    to_date(created_at)             as event_date,
    is_public,
    actor_id,
    repo_id,
    org_id,
    nullif(trim(action), '')        as action,
    nullif(trim(ref), '')           as ref,
    nullif(trim(ref_type), '')      as ref_type,
    push_id,
    push_size,
    push_distinct_size,
    pr_number,
    pr_id,
    nullif(trim(pr_state), '')      as pr_state,
    pr_merged,
    pr_created_at,
    pr_merged_at,
    issue_id,
    issue_number,
    nullif(trim(issue_state), '')   as issue_state,
    comment_id,
    forkee_id,
    release_id,
    member_id
from {{ source('raw', 'EVENTS') }}
