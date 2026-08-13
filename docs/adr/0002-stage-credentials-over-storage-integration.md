# ADR-0002: External stage with embedded credentials over a storage integration

**Status:** Accepted
**Date:** 2026-08-13

## Context

Snowflake needs to read the Parquet files that back the S3 Delta table. Two
ways to authorize that read:

1. **Embedded credentials on the stage** -- `CREATE STAGE ... CREDENTIALS =
   (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...')`. The IAM user's access key and
   secret are given directly to the stage object.
2. **Storage integration** -- Snowflake creates an IAM role reference, hands
   back its own AWS account ID and an external ID, and the S3 side grants that
   specific Snowflake-owned identity read access via an S3 bucket policy or a
   trust relationship. No long-lived key ever enters Snowflake.

Storage integration is the correct answer for a real deployment: it removes a
static credential that could leak from a config file or a query history entry,
and access can be revoked by editing a trust policy rather than rotating a key
everywhere it was pasted.

## Decision

Use an external stage with embedded credentials, reusing the `gitlake-pipeline`
IAM user's existing keys (already scoped to this bucket only, already sitting
in `.env`).

## Alternatives considered

**Storage integration.** Rejected for this project, not as a security
judgment but a scope one. Setting it up means: run `CREATE STORAGE
INTEGRATION`, read back the auto-generated `STORAGE_AWS_IAM_USER_ARN` and
`STORAGE_AWS_EXTERNAL_ID` Snowflake assigns, create a *second* AWS IAM role
(distinct from `gitlake-pipeline`) with a trust policy naming those two
values, then point the stage at the integration instead of a key pair. That
is a real, defensible piece of infrastructure -- and also a second IAM
identity to explain, in a project whose non-negotiables (see CLAUDE.md §9)
are already Spark/Kafka/Delta/dbt/Airflow, not IAM trust policy design.

## Consequences

**Cost -- a real secret sits in a `CREATE STAGE` statement.** Mitigated the
way every other secret in this repo is: the committed `setup.sql` holds
placeholders (`{AWS_ACCESS_KEY_ID}`, `{AWS_SECRET_ACCESS_KEY}`), never real
values. `snowflake/run_sql.py` substitutes them from `.env` at execution time
and the resolved SQL is never written to disk. The key itself is the
`gitlake-pipeline` IAM user, already least-privilege: read/write/delete on one
bucket, nothing else in the AWS account.

**Cost -- if this key rotates, the stage needs `ALTER STAGE ... SET
CREDENTIALS` too.** One extra step on rotation, acceptable for a key that
does not rotate on any schedule here.

**Benefit -- honest tradeoff, not a hidden one.** The interview answer this
buys: "storage integration is what I'd run in production, and here is
specifically why I didn't build it for a single-developer portfolio pipeline"
is a stronger answer than either silently using the weaker option or building
unnecessary infrastructure to avoid explaining the tradeoff.
