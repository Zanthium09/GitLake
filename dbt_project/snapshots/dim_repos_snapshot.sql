-- SCD Type 2 history of repo name/owner over time.
--
-- Targets stg_repos, not dim_repos. A snapshot's job is to detect and
-- historize changes in a source of current-state truth -- stg_repos already
-- is that (one row per repo_id, latest known name). dim_repos is a
-- downstream aggregate carrying activity counts alongside identity
-- attributes; snapshotting it would mean every activity-count change looks
-- like a new SCD version, which is not what "track when a repo was renamed"
-- means. Snapshotting the identity-only staging model keeps the two concerns
-- (activity summary vs. name history) from bleeding into each other.
--
-- strategy='check' rather than 'timestamp': stg_repos has no reliable
-- updated_at column -- repo_name only changes when GitHub reports it
-- differently between events, which this project has no independent
-- timestamp for. check compares the named columns on every snapshot run and
-- opens a new SCD row when any of them differ from the last snapshot.
--
-- Run: dbt snapshot

{% snapshot dim_repos_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='repo_id',
        strategy='check',
        check_cols=['repo_name', 'repo_owner', 'repo_short_name'],
    )
}}

select * from {{ ref('stg_repos') }}

{% endsnapshot %}
