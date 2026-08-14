"""The pipeline, orchestrated: stream_ingest -> to_delta -> load_snowflake -> dbt_build.

Each task shells out to the exact scripts already tested from the WSL venv --
this DAG's job is scheduling, retries, and backfill, not reimplementing any
pipeline logic. The repo is mounted read-only into the container at
/usr/local/airflow/gitlake (see docker-compose.override.yml); requirements.txt
and packages.txt install the same Python/Java toolchain into the image so
those scripts run unmodified.

Two things worth knowing before reading the tasks:

localhost means something different inside this container, and so does
host.docker.internal for Kafka specifically. Both were tried: localhost:9092
is wrong because it means this container, not Kafka's. host.docker.internal
resolves and completes the initial connection, but Kafka's EXTERNAL listener
then advertises itself back to the client as localhost:9092 -- correct for a
process on the WSL host, a dead address from inside a different container --
so the client times out fetching metadata after the handshake succeeds.
Every task that touches Kafka instead overrides --bootstrap to
kafka:29092, the INTERNAL listener, reachable because docker-compose.override.yml
joins the scheduler to gitlake_default (Kafka's own compose network) directly.
Same approach Kafka UI already used correctly from the start.

dbt test failures fail the task because dbt build does that by default, not
because of anything added here. dbt build exits non-zero when an
error-severity test fails, and BashOperator raises on any non-zero exit --
that pairing is the entire mechanism behind CLAUDE.md's non-negotiable #1
("a failed test fails the Airflow task, not a warning"). The one test in this
project actually configured at severity: warn (stg_events' event_id
uniqueness, see dbt_project/models/staging/_staging.yml) does NOT fail the
task, deliberately -- see that file for why.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

GITLAKE = "/usr/local/airflow/gitlake"

# Every task needs secrets (AWS keys, Kafka address, Snowflake creds) that
# live in .env, mounted read-only alongside the code. Loading it inline
# rather than baking values into Airflow Connections keeps exactly one place
# (.env) as the source of truth for every environment this project runs in --
# the WSL venv, ad hoc scripts, and now these tasks.
LOAD_ENV = f"set -a && . {GITLAKE}/.env && set +a && "

default_args = {
    "owner": "gitlake",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="gitlake_pipeline",
    description="GitHub Archive: Kafka -> Spark -> Delta -> Snowflake -> dbt",
    default_args=default_args,
    start_date=datetime(2024, 1, 15),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["gitlake"],
) as dag:

    stream_ingest = BashOperator(
        task_id="stream_ingest",
        bash_command=(
            LOAD_ENV
            + f"cd {GITLAKE} && "
            # If the day is already sitting in the mounted host cache
            # (data/raw/, gitignored, built up over earlier local testing),
            # use it directly rather than re-downloading. Found this the hard
            # way: this container's outbound connection to gharchive.org
            # stalled mid-file (a .part sitting at the same byte count for
            # several minutes) even though DNS and the Kafka network both
            # worked fine -- container networking flakiness unrelated to the
            # Kafka fix above. The host cache sidesteps it entirely for any
            # day already fetched. New days (a real backfill beyond what's
            # cached) still fall through to downloading into /tmp -- that
            # mount is read-only, and raw JSON is gitignored scratch data
            # anyway, not something a container run should write back into
            # the repo.
            + "if ls " + GITLAKE + "/data/raw/{{ ds }}-*.json.gz "
            + ">/dev/null 2>&1; then "
            + "RAW_DIR=" + GITLAKE + "/data/raw; "
            + "else "
            + "python ingest/download.py --day {{ ds }} "
            + "--dest /tmp/gitlake_data/raw && "
            + "RAW_DIR=/tmp/gitlake_data/raw; "
            + "fi && "
            + "python ingest/producer.py "
            + "--input \"$RAW_DIR/{{ ds }}-*.json.gz\" "
            + "--bootstrap kafka:29092 "
            + "--rate 0"
        ),
    )

    to_delta = BashOperator(
        task_id="to_delta",
        bash_command=(
            LOAD_ENV
            + f"cd {GITLAKE} && "
            + "python spark/stream_to_delta.py "
            + "--output s3a://$S3_BUCKET/delta/events "
            + "--checkpoint s3a://$S3_BUCKET/checkpoints/events "
            + "--bootstrap kafka:29092 "
            + "--metrics-out ''"
        ),
    )

    load_snowflake = BashOperator(
        task_id="load_snowflake",
        bash_command=(
            LOAD_ENV
            + f"cd {GITLAKE} && "
            # setup.sql is CREATE ... IF NOT EXISTS / OR REPLACE throughout,
            # safe to rerun every day rather than only once.
            + "python snowflake/run_sql.py snowflake/setup.sql && "
            # Must run before copy_into.sql on every load, not just the
            # first -- COPY INTO against the raw Delta path double-counts
            # otherwise. See spark/export_for_snowflake.py.
            + "python spark/export_for_snowflake.py && "
            + "python snowflake/run_sql.py snowflake/copy_into.sql"
        ),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            LOAD_ENV
            # profiles.yml.example has no secrets in it -- every value is an
            # env_var() call -- so it is copied as-is rather than needing a
            # second, container-specific profile. It cannot be used directly
            # from the mounted directory: that mount is read-only, and dbt
            # requires the file to be named exactly profiles.yml.
            + "mkdir -p /tmp/dbt_profiles && "
            + f"cp {GITLAKE}/dbt_project/profiles.yml.example "
            + "/tmp/dbt_profiles/profiles.yml && "
            + f"cd {GITLAKE}/dbt_project && "
            # --target-path and --log-path are both load-bearing, not
            # defensive: dbt writes compiled SQL/run results to target/ and
            # its own dbt.log to logs/ inside the project directory by
            # default, and both live under the same read-only mount as
            # profiles.yml.example -- failed on PermissionError against
            # logs/dbt.log before this was added, and target/ would have hit
            # the identical error on the very next write.
            + "dbt build --profiles-dir /tmp/dbt_profiles --profile gitlake "
            + "--target-path /tmp/dbt_target --log-path /tmp/dbt_logs"
        ),
    )

    stream_ingest >> to_delta >> load_snowflake >> dbt_build
