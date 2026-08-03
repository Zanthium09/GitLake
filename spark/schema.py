"""Explicit schemas for GitHub Archive events.

Why explicit instead of inferSchema: inference makes Spark read the whole file
once just to learn the column names, then again to load it -- double the I/O.
Worse, the result depends on what happened to be in the sample, so a rare field
missing from one hour silently changes the schema of that run's output.

The shape of the data:

    every event  ->  id, type, actor, repo, org, payload, public, created_at
    payload      ->  completely different fields per event type

PushEvent payload carries commit data. WatchEvent payload is nearly empty.
PullRequestEvent payload nests an entire pull request object.

Rather than branch on `type` and apply 15 different schemas, PAYLOAD_SCHEMA is
the union of the fields worth keeping from any type, and every field is
nullable. Spark fills in null for whatever an event does not have, so reading a
WatchEvent through this schema simply leaves the push columns empty.

The tradeoff, stated plainly: the flattened table is wide and sparse. That is
fine here because the columns are cheap -- Parquet stores a null column as a
run-length-encoded stub, not as 4.7M nulls. It would not be fine if the union
grew to hundreds of fields.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Present on every event. `id` is a string in the archive even though it is
# numeric -- keeping it as a string avoids a silent overflow question and it is
# only ever used as a key, never arithmetic.
ACTOR_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("display_login", StringType(), True),
        StructField("url", StringType(), True),
        StructField("avatar_url", StringType(), True),
    ]
)

REPO_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# Only present when the event happened inside an organisation, so this whole
# struct is null for most personal-repo activity.
ORG_SCHEMA = StructType(
    [
        StructField("id", LongType(), True),
        StructField("login", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

PAYLOAD_SCHEMA = StructType(
    [
        # Shared by most action-style events (opened / closed / created / ...).
        StructField("action", StringType(), True),
        # CreateEvent, DeleteEvent, PushEvent.
        StructField("ref", StringType(), True),
        StructField("ref_type", StringType(), True),
        # PushEvent. `size` is total commits, `distinct_size` excludes commits
        # already seen on another branch -- they differ on merges and force
        # pushes, which is exactly the case worth measuring.
        StructField("push_id", LongType(), True),
        StructField("size", IntegerType(), True),
        StructField("distinct_size", IntegerType(), True),
        StructField("head", StringType(), True),
        StructField("before", StringType(), True),
        # PullRequestEvent puts the number at payload level; IssuesEvent puts
        # it inside `issue`. Both are kept and reconciled during flattening.
        StructField("number", IntegerType(), True),
        StructField(
            "pull_request",
            StructType(
                [
                    StructField("id", LongType(), True),
                    StructField("state", StringType(), True),
                    StructField("merged", BooleanType(), True),
                    StructField("created_at", TimestampType(), True),
                    StructField("merged_at", TimestampType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "issue",
            StructType(
                [
                    StructField("id", LongType(), True),
                    StructField("number", IntegerType(), True),
                    StructField("state", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "comment",
            StructType([StructField("id", LongType(), True)]),
            True,
        ),
        StructField(
            "forkee",
            StructType(
                [
                    StructField("id", LongType(), True),
                    StructField("full_name", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "release",
            StructType(
                [
                    StructField("id", LongType(), True),
                    StructField("tag_name", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "member",
            StructType(
                [
                    StructField("id", LongType(), True),
                    StructField("login", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("type", StringType(), True),
        StructField("actor", ACTOR_SCHEMA, True),
        StructField("repo", REPO_SCHEMA, True),
        StructField("org", ORG_SCHEMA, True),
        StructField("payload", PAYLOAD_SCHEMA, True),
        StructField("public", BooleanType(), True),
        # Archive writes ISO-8601 with a Z suffix ("2024-01-15T00:00:00Z"),
        # which Spark parses into TimestampType without a format string.
        StructField("created_at", TimestampType(), True),
    ]
)

# Every type seen in 2024-01-15 hour 0. Used to assert that a run has not
# encountered an event type this schema was never checked against -- GitHub has
# added and retired event types over the years, and a silent new one is how a
# pipeline starts dropping data without failing.
KNOWN_EVENT_TYPES = frozenset(
    {
        "CommitCommentEvent",
        "CreateEvent",
        "DeleteEvent",
        "ForkEvent",
        "GollumEvent",
        "IssueCommentEvent",
        "IssuesEvent",
        "MemberEvent",
        "PublicEvent",
        "PullRequestEvent",
        "PullRequestReviewCommentEvent",
        "PullRequestReviewEvent",
        "PushEvent",
        "ReleaseEvent",
        "WatchEvent",
    }
)
