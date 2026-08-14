# ADR-0003: Star schema grain and incremental strategy

**Status:** Accepted
**Date:** 2026-08-13

## Context

Week 3 needed a star schema over the flattened event data already sitting in
Snowflake, plus a materialization strategy that supports rerunning a day
without doubling it (the same idempotency requirement that shaped
`batch_to_delta.py`'s `replaceWhere` in week 1).

Two decisions had to be made together, not separately: what one row of the
fact table means, and how new data gets into these tables on a rerun.

## Decision

**Grain: `fct_events` is one row per `event_id`.** `dim_repos` is one row per
`repo_id`, `dim_actors` one row per `actor_id`, both current-state
(attributes + activity summary), not historized.

**Materialization: `incremental` with `incremental_strategy='merge'`, keyed on
each table's natural id**, for all three marts.

## Alternatives considered

**Coarser fact grain (one row per repo-day, matching
`int_daily_repo_activity`).** Rejected. Week 5 analysis 2 is hour-of-day and
day-of-week seasonality, which needs the event's own timestamp, not a daily
bucket someone already collapsed it into. A fact table has to be the finest
grain any planned query needs; aggregating early forecloses that.

**`incremental_strategy='append'` instead of `merge`.** Rejected outright --
append has no concept of "this row already exists," so reprocessing the same
day is not idempotent by construction. Every rerun would double the table.
Merge was the only candidate that satisfies CLAUDE.md's non-negotiable #2.

## Consequences

**A grain enforced only by the load strategy is not actually enforced.**
Found this the hard way: `fct_events`'s first build is a plain `CREATE TABLE
AS SELECT`, not a merge -- `dbt`'s incremental models only run merge logic on
the *second* build onward, once the target table exists. The one confirmed
upstream duplicate event (id `34822912958`, GitHub Archive itself emitted it
twice) sailed straight through the initial load and failed the `unique` test.
The fix is a `qualify row_number() ... = 1` inside `fct_events.sql` itself, so
the grain holds on every build path -- first load, incremental load, and
`--full-refresh` alike -- rather than depending on which one happened to run.
The lesson generalizes: a fact table's declared grain needs to be true of the
query, not an emergent property of the materialization config.

**Dimensions are current-state, not historized, except where a snapshot
exists.** `dim_repos` and `dim_actors` show today's attributes; there is no
`dim_repos_valid_from` here. History is deliberately pushed to
`snapshots/dim_repos_snapshot.sql` instead (see that file's own comment for
why it targets `stg_repos`, not `dim_repos`), keeping "what does this repo
look like now" and "when did its name change" as two separate, single-purpose
objects rather than one table trying to answer both.

**Cost -- every incremental model re-scans its full source on the first
build.** Acceptable at one week of data; would need a bounded initial
backfill window at a larger scale.
