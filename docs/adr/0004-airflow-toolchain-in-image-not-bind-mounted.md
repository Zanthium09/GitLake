# ADR-0004: Install the pipeline toolchain into the Airflow image, mount only source

**Status:** Accepted
**Date:** 2026-08-14

## Context

Airflow's DAG tasks need to run the same Spark, dbt, and Python scripts
already built and tested from the WSL venv (`ingest/`, `spark/`,
`snowflake/`, `dbt_project/`). Astro CLI runs Airflow in Docker containers on
Astro Runtime, a different base image from the Ubuntu 22.04 WSL install these
scripts were developed and tested against.

Two ways to give the containers access to that toolchain:

1. **Bind-mount the host's `.venv` and Java installation** into the
   containers, reusing the exact binaries already built and tested.
2. **Install the same packages fresh inside the image** (via
   `packages.txt` for `openjdk-17-jdk-headless`, `requirements.txt` for
   `pyspark`, `delta-spark`, `dbt-snowflake`, etc.), and mount only the
   *source code* (`spark/*.py`, `dbt_project/`, `.env`) as a read-only
   volume.

## Decision

Install fresh inside the image. Mount source code only.

## Alternatives considered

**Bind-mounting the host `.venv` and JVM.** Rejected. A WSL host binary and
an Astro Runtime container do not share a base OS -- different glibc,
different set of shared libraries already present. A JVM built against one
does not reliably run against the other; pip-installed packages with
compiled extensions (`pyarrow`, parts of `pyspark`) carry the same risk.
This would have meant debugging binary compatibility issues with no clear
error message pointing at the actual cause, for a shortcut that saves one
`requirements.txt` edit.

## Consequences

**Cost -- the image is heavier and the first build is slow.** Installing a
JDK plus the full Spark/dbt/Snowflake Python stack adds real build time on
`astro dev start`. Accepted: this cost is paid once per environment, not
per DAG run.

**Cost -- two `requirements.txt` files now exist** (`requirements.txt` at
the repo root for the WSL venv, `airflow/requirements.txt` for the
container), and they need to be kept in sync by hand. No tooling enforces
this; a drift would show up as a task that works from the WSL venv but fails
inside Airflow, or vice versa. Acceptable at this project's size -- both
files list the same seven packages.

**Benefit -- tasks run the actual repo, not a snapshot of it.** The volume
mount is read-only and points at the live source tree, so a code change is
visible to the next DAG run without rebuilding the image. Only a
`requirements.txt`/`packages.txt` change needs a rebuild.

**A related, separate problem this does not solve:** the Airflow containers
are not on the same Docker network as `docker-compose.yml`'s Kafka
container, and `localhost` means something different inside each container.
Tasks that need Kafka pass `--bootstrap host.docker.internal:9092`
explicitly rather than relying on `.env`'s `KAFKA_BOOTSTRAP_SERVERS=
localhost:9092`, which is correct for host-side scripts but wrong inside any
container. See the comment block at the top of
`airflow/dags/gitlake_pipeline.py`.
