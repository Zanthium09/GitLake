-- Database, schema, and the external stage that lets Snowflake read the
-- Delta table's Parquet files directly off S3. Run once per environment.
--
-- {AWS_ACCESS_KEY_ID} / {AWS_SECRET_ACCESS_KEY} / {S3_BUCKET} are placeholders,
-- not real values -- this file is committed to git. They are substituted from
-- .env at execution time and the resolved SQL is never written to disk. See
-- docs/adr/0002-stage-credentials-over-storage-integration.md for why this
-- project uses embedded stage credentials instead of a storage integration.
--
-- Run: python snowflake/run_sql.py snowflake/setup.sql

CREATE DATABASE IF NOT EXISTS GITLAKE;

CREATE SCHEMA IF NOT EXISTS GITLAKE.RAW;

-- Matches what the console already has (X-Small, 60s auto-suspend) so the
-- warehouse is reproducible from this file rather than only existing because
-- someone clicked through Snowsight once. IF NOT EXISTS makes this safe to
-- rerun without fighting the warehouse already made in the UI.
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE FILE FORMAT IF NOT EXISTS GITLAKE.RAW.PARQUET_FORMAT
    TYPE = PARQUET;

-- Points at delta/events_snowflake/, NOT delta/events/ (the pipeline's real
-- table). Delta keeps every historical data file for time travel -- an
-- overwrite stops referencing old files in _delta_log but does not delete
-- them. COPY INTO has no _delta_log awareness and just lists every *.parquet
-- file under the stage URL, so pointed at the real table it loads every
-- superseded file from every run the table has ever seen (measured: 11.45M
-- rows loaded against a table that holds 5.53M). events_snowflake/ is a
-- single-version snapshot written by spark/export_for_snowflake.py
-- specifically so file count matches row count. Re-run that script before
-- every load; this stage always reads whatever it last produced.
--
-- CREATE OR REPLACE rather than IF NOT EXISTS: a stage is just a pointer and
-- credentials, nothing is lost by resetting it to this file's definition on
-- every run, which matters if the URL or keys ever change.
CREATE OR REPLACE STAGE GITLAKE.RAW.EVENTS_STAGE
    URL = 's3://{S3_BUCKET}/delta/events_snowflake/'
    CREDENTIALS = (
        AWS_KEY_ID = '{AWS_ACCESS_KEY_ID}'
        AWS_SECRET_KEY = '{AWS_SECRET_ACCESS_KEY}'
    )
    FILE_FORMAT = GITLAKE.RAW.PARQUET_FORMAT;

-- Mirrors flatten()'s output columns in spark/batch_to_delta.py exactly.
-- NUMBER rather than INTEGER/BIGINT because Snowflake stores all of them
-- identically (NUMBER(38,0)) regardless -- the distinction is cosmetic here.
CREATE TABLE IF NOT EXISTS GITLAKE.RAW.EVENTS (
    event_id            STRING,
    event_type          STRING,
    created_at          TIMESTAMP_NTZ,  -- UTC; see the Spark timezone fix ADR
    event_date          DATE,
    is_public           BOOLEAN,
    actor_id            NUMBER,
    actor_login         STRING,
    actor_display_login STRING,
    repo_id             NUMBER,
    repo_name           STRING,
    org_id              NUMBER,
    org_login           STRING,
    action              STRING,
    ref                 STRING,
    ref_type            STRING,
    push_id             NUMBER,
    push_size           NUMBER,
    push_distinct_size  NUMBER,
    push_head           STRING,
    push_before         STRING,
    pr_number           NUMBER,
    pr_id               NUMBER,
    pr_state            STRING,
    pr_merged           BOOLEAN,
    pr_created_at       TIMESTAMP_NTZ,
    pr_merged_at        TIMESTAMP_NTZ,
    issue_id            NUMBER,
    issue_number        NUMBER,
    issue_state         STRING,
    comment_id          NUMBER,
    forkee_id           NUMBER,
    forkee_full_name    STRING,
    release_id          NUMBER,
    release_tag         STRING,
    member_id           NUMBER,
    member_login        STRING
);
