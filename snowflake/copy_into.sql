-- Loads the Delta table's Parquet files into GITLAKE.RAW.EVENTS.
-- Run: python snowflake/run_sql.py snowflake/copy_into.sql

USE WAREHOUSE COMPUTE_WH;
USE DATABASE GITLAKE;
USE SCHEMA RAW;

-- TRUNCATE first -- load-bearing, not defensive. export_for_snowflake.py
-- overwrites its export path on every run, and Spark gives each write fresh
-- Parquet filenames (new UUIDs). Snowflake's COPY INTO tracks "already
-- loaded" by file name, so a rerun never recognizes yesterday's files as
-- seen and reloads everything under new names -- doubling the table on a
-- second run rather than replacing it. This statement is what makes
-- rerunning the load idempotent; without it the S3/Delta side can be
-- perfectly correct while Snowflake silently drifts to 2x, 3x, ... counts.
TRUNCATE TABLE GITLAKE.RAW.EVENTS;

-- PATTERN restricts the file listing to the actual data files. Without it,
-- COPY INTO also tries to parse _delta_log/*.json as Parquet and fails the
-- whole load on the first commit file it meets.
--
-- ON_ERROR = 'ABORT_STATEMENT' (the default) is deliberate, not an oversight:
-- a partially loaded day is worse than a failed COPY INTO, because a partial
-- load succeeds silently and the row-count mismatch is the only symptom.
COPY INTO GITLAKE.RAW.EVENTS
FROM @GITLAKE.RAW.EVENTS_STAGE
PATTERN = '.*\\.parquet'
FILE_FORMAT = GITLAKE.RAW.PARQUET_FORMAT
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT COUNT(*) AS row_count FROM GITLAKE.RAW.EVENTS;
